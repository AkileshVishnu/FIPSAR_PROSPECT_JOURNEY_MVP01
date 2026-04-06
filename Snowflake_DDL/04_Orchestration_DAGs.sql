-- ============================================================================
-- PART E: TASK DAG  (08_ORCHESTRATION_AND_TASKS.sql)
-- FIX-08 APPLIED:
--   - Email SP defined above (before any task)
--   - TASK_GOLD_LOAD now chains AFTER TASK_SILVER_DQ_NOTIFICATION (not SILVER_LOAD)
--     This serialises: Silver → Email → Gold → Outbound → Completion notice
--   - All child tasks resumed before parent (correct Snowflake pattern)
-- ============================================================================
USE DATABASE FIPSAR_PHI_HUB;
USE SCHEMA PHI_CORE;

-- ROOT: Every 5 minutes. Loads S3 CSVs into PHI staging.
CREATE OR REPLACE TASK TASK_PHI_ORCHESTRATOR
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = '5 MINUTE'
    COMMENT   = 'ROOT: Load S3 CSVs into PHI staging'
AS
    CALL FIPSAR_PHI_HUB.PHI_CORE.SP_LOAD_STAGING_FROM_S3();

-- Chain 1: PHI identity resolution + load
CREATE OR REPLACE TASK TASK_PHI_IDENTITY_LOAD
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'Assign MASTER_PATIENT_ID and load PHI table'
    AFTER     TASK_PHI_ORCHESTRATOR
AS
    CALL FIPSAR_PHI_HUB.PHI_CORE.SP_LOAD_PHI_PROSPECT();

-- Chain 2: Bronze raw copy
CREATE OR REPLACE TASK TASK_BRONZE_LOAD
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'Raw copy PHI → Bronze'
    AFTER     TASK_PHI_IDENTITY_LOAD
AS
    CALL FIPSAR_DW.BRONZE.SP_LOAD_BRONZE_PROSPECT();

-- Chain 3: Silver DQ gates
CREATE OR REPLACE TASK TASK_SILVER_LOAD
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'DQ gates: null rejection + dedup + SCD2 → Silver'
    AFTER     TASK_BRONZE_LOAD
AS
    CALL FIPSAR_DW.SILVER.SP_LOAD_SILVER_PROSPECT();



-- Chain 4b: Gold load (FIX-08: now runs AFTER email, not in parallel with it)
CREATE OR REPLACE TASK TASK_GOLD_LOAD
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'Load Gold dimensions and facts'
    AFTER     TASK_SILVER_LOAD
AS
BEGIN
    CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_PROSPECT();
    CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_INTAKE();
END;

-- Chain 5: SFMC outbound export
CREATE OR REPLACE TASK TASK_SFMC_OUTBOUND_EXPORT
    WAREHOUSE = COMPUTE_WH
    COMMENT   = 'Export prospect delta CSV to S3 for SFMC (Prospect_c_delta_YYYYMMDD_HHMM.csv)'
    AFTER     TASK_GOLD_LOAD
AS
    CALL FIPSAR_DW.GOLD.SP_EXPORT_SFMC_OUTBOUND();




-- Separate root: SFMC event ingestion (every 4 hours)
CREATE OR REPLACE TASK FIPSAR_SFMC_EVENTS.RAW_EVENTS.TASK_SFMC_EVENTS_LOAD
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 */4 * * * UTC'
    COMMENT   = 'Load SFMC event files from dedicated S3 folders every 4 hours'
AS
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_RUN_SFMC_EVENTS_PIPELINE();

-- ============================================================================
-- STEP: RESUME ALL TASKS
-- IMPORTANT: Resume child tasks BEFORE parent. Root task last.
-- ============================================================================

ALTER TASK TASK_SFMC_OUTBOUND_EXPORT         RESUME;
ALTER TASK TASK_GOLD_LOAD                    RESUME;
--ALTER TASK TASK_SILVER_DQ_NOTIFICATION       RESUME;
ALTER TASK TASK_SILVER_LOAD                  RESUME;
ALTER TASK TASK_BRONZE_LOAD                  RESUME;
ALTER TASK TASK_PHI_IDENTITY_LOAD            RESUME;
ALTER TASK TASK_PHI_ORCHESTRATOR             RESUME;   -- Root last
ALTER TASK FIPSAR_SFMC_EVENTS.RAW_EVENTS.TASK_SFMC_EVENTS_LOAD RESUME;

-- ============================================================================
-- VERIFY TASKS
-- ============================================================================
SHOW TASKS IN SCHEMA FIPSAR_PHI_HUB.PHI_CORE;
SHOW TASKS IN SCHEMA FIPSAR_SFMC_EVENTS.RAW_EVENTS;

-- ============================================================================
-- Suspend TASKS
-- ============================================================================

use database FIPSAR_PHI_HUB;
use schema PHI_CORE;




ALTER TASK TASK_PHI_ORCHESTRATOR             SUSPEND;
ALTER TASK TASK_PHI_IDENTITY_LOAD            SUSPEND;
ALTER TASK TASK_BRONZE_LOAD                  SUSPEND;
ALTER TASK TASK_SILVER_LOAD                  SUSPEND;
ALTER TASK TASK_GOLD_LOAD                    SUSPEND;
ALTER TASK TASK_SFMC_OUTBOUND_EXPORT         SUSPEND;
--ALTER TASK TASK_PIPELINE_NOTIFICATION        SUSPEND;
ALTER TASK FIPSAR_SFMC_EVENTS.RAW_EVENTS.TASK_SFMC_EVENTS_LOAD SUSPEND;