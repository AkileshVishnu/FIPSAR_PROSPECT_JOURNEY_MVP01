

-- ============================================================================
-- ENHANCED PROCEDURE 1: LOAD STAGING FROM S3 (INCREMENTAL, NO TRUNCATE)
-- No schema/column changes
-- ============================================================================
CREATE OR REPLACE PROCEDURE FIPSAR_PHI_HUB.PHI_CORE.SP_LOAD_STAGING_FROM_S3()
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded INTEGER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS)
    VALUES
        (:v_run_id, 'SP_LOAD_STAGING_FROM_S3', 'PHI', 'STG_PROSPECT_INTAKE', 'STARTED');

    COPY INTO FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    (
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        AGE,
        ADDRESS,
        APARTMENT_NO,
        CITY,
        STATE,
        ZIP_CODE,
        PATIENT_CONSENT,
        CHANNEL,
        SUBMISSION_TIMESTAMP,
        FILE_DATE,
        CREATED_AT,
        SOURCE_FILE_NAME
    )
    FROM
    (
        SELECT
            $1::STRING AS FIRST_NAME,
            $2::STRING AS LAST_NAME,
            $3::STRING AS EMAIL,
            $4::STRING AS PHONE_NUMBER,
            $5::STRING AS AGE,
            $6::STRING AS ADDRESS,
            $7::STRING AS APARTMENT_NO,
            $8::STRING AS CITY,
            $9::STRING AS STATE,
            $10::STRING AS ZIP_CODE,
            $11::STRING AS PATIENT_CONSENT,
            $12::STRING AS CHANNEL,
            $13::STRING AS SUBMISSION_TIMESTAMP,
            $14::STRING AS FILE_DATE,
            CURRENT_TIMESTAMP() AS CREATED_AT,
            METADATA$FILENAME AS SOURCE_FILE_NAME
        FROM @FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INBOUND
    )
    FILE_FORMAT = (FORMAT_NAME = FIPSAR_PHI_HUB.STAGING.FF_PROSPECT_CSV)
    PATTERN     = '.*prospect_campaign_.*[.]csv'
    ON_ERROR    = CONTINUE
    FORCE       = FALSE;

    SELECT COUNT(*)
    INTO :v_rows_loaded
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        COMPLETED_AT = CURRENT_TIMESTAMP(),
        ERROR_MESSAGE = NULL
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_present_in_staging', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_STAGING_FROM_S3 failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_LOAD_STAGING_FROM_S3 failed'
        );
END;
$$;


-- ============================================================================
-- ENHANCED PROCEDURE 2: LOAD PHI PROSPECT
-- Adds DQ rejection logging + phone validation + safe error handling
-- No schema/column changes
-- ============================================================================
CREATE OR REPLACE PROCEDURE FIPSAR_PHI_HUB.PHI_CORE.SP_LOAD_PHI_PROSPECT()
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id              VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded         INTEGER DEFAULT 0;
    v_new_ids             INTEGER DEFAULT 0;
    v_rejected_rows       INTEGER DEFAULT 0;
    v_null_first_name     INTEGER DEFAULT 0;
    v_null_last_name      INTEGER DEFAULT 0;
    v_null_email          INTEGER DEFAULT 0;
    v_null_phone          INTEGER DEFAULT 0;
    v_invalid_file_date   INTEGER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS)
    VALUES
        (:v_run_id, 'SP_LOAD_PHI_PROSPECT', 'PHI', 'PHI_PROSPECT_MASTER', 'STARTED');

    -- ------------------------------------------------------------------------
    -- STEP 1: Log rejected rows to DQ_REJECTION_LOG
    -- ------------------------------------------------------------------------

    -- NULL_FIRST_NAME
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        'PHI_PROSPECT_MASTER' AS TABLE_NAME,
        'NULL_FIRST_NAME' AS REJECTION_REASON,
        OBJECT_CONSTRUCT(
            'FIRST_NAME', FIRST_NAME,
            'LAST_NAME', LAST_NAME,
            'EMAIL', EMAIL,
            'PHONE_NUMBER', PHONE_NUMBER,
            'CHANNEL', CHANNEL,
            'FILE_DATE', FILE_DATE,
            'STAGE', 'PHI_LOAD'
        ) AS REJECTED_RECORD,
        CURRENT_TIMESTAMP() AS REJECTED_AT
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE NULLIF(TRIM(FIRST_NAME), '') IS NULL;

    v_null_first_name := SQLROWCOUNT;

    -- NULL_LAST_NAME
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        'PHI_PROSPECT_MASTER',
        'NULL_LAST_NAME',
        OBJECT_CONSTRUCT(
            'FIRST_NAME', FIRST_NAME,
            'LAST_NAME', LAST_NAME,
            'EMAIL', EMAIL,
            'PHONE_NUMBER', PHONE_NUMBER,
            'CHANNEL', CHANNEL,
            'FILE_DATE', FILE_DATE,
            'STAGE', 'PHI_LOAD'
        ),
        CURRENT_TIMESTAMP()
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE NULLIF(TRIM(FIRST_NAME), '') IS NOT NULL
      AND NULLIF(TRIM(LAST_NAME), '') IS NULL;

    v_null_last_name := SQLROWCOUNT;

    -- NULL_EMAIL
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        'PHI_PROSPECT_MASTER',
        'NULL_EMAIL',
        OBJECT_CONSTRUCT(
            'FIRST_NAME', FIRST_NAME,
            'LAST_NAME', LAST_NAME,
            'EMAIL', EMAIL,
            'PHONE_NUMBER', PHONE_NUMBER,
            'CHANNEL', CHANNEL,
            'FILE_DATE', FILE_DATE,
            'STAGE', 'PHI_LOAD'
        ),
        CURRENT_TIMESTAMP()
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE NULLIF(TRIM(FIRST_NAME), '') IS NOT NULL
      AND NULLIF(TRIM(LAST_NAME), '') IS NOT NULL
      AND NULLIF(TRIM(EMAIL), '') IS NULL;

    v_null_email := SQLROWCOUNT;

    -- NULL_PHONE_NUMBER
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        'PHI_PROSPECT_MASTER',
        'NULL_PHONE_NUMBER',
        OBJECT_CONSTRUCT(
            'FIRST_NAME', FIRST_NAME,
            'LAST_NAME', LAST_NAME,
            'EMAIL', EMAIL,
            'PHONE_NUMBER', PHONE_NUMBER,
            'CHANNEL', CHANNEL,
            'FILE_DATE', FILE_DATE,
            'STAGE', 'PHI_LOAD'
        ),
        CURRENT_TIMESTAMP()
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE NULLIF(TRIM(FIRST_NAME), '') IS NOT NULL
      AND NULLIF(TRIM(LAST_NAME), '') IS NOT NULL
      AND NULLIF(TRIM(EMAIL), '') IS NOT NULL
      AND NULLIF(TRIM(PHONE_NUMBER), '') IS NULL;

    v_null_phone := SQLROWCOUNT;

    -- INVALID_FILE_DATE
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME,
        REJECTION_REASON,
        REJECTED_RECORD,
        REJECTED_AT
    )
    SELECT
        'PHI_PROSPECT_MASTER',
        'INVALID_FILE_DATE',
        OBJECT_CONSTRUCT(
            'FIRST_NAME', FIRST_NAME,
            'LAST_NAME', LAST_NAME,
            'EMAIL', EMAIL,
            'PHONE_NUMBER', PHONE_NUMBER,
            'CHANNEL', CHANNEL,
            'FILE_DATE', FILE_DATE,
            'STAGE', 'PHI_LOAD'
        ),
        CURRENT_TIMESTAMP()
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
    WHERE NULLIF(TRIM(FIRST_NAME), '') IS NOT NULL
      AND NULLIF(TRIM(LAST_NAME), '') IS NOT NULL
      AND NULLIF(TRIM(EMAIL), '') IS NOT NULL
      AND NULLIF(TRIM(PHONE_NUMBER), '') IS NOT NULL
      AND COALESCE(
            TRY_TO_DATE(FILE_DATE, 'YYYY-MM-DD'),
            TRY_TO_DATE(FILE_DATE, 'DD-MM-YYYY'),
            TRY_TO_DATE(FILE_DATE)
          ) IS NULL;

    v_invalid_file_date := SQLROWCOUNT;

    v_rejected_rows := COALESCE(v_null_first_name, 0)
                     + COALESCE(v_null_last_name, 0)
                     + COALESCE(v_null_email, 0)
                     + COALESCE(v_null_phone, 0)
                     + COALESCE(v_invalid_file_date, 0);

    -- ------------------------------------------------------------------------
    -- STEP 2: Merge only valid identities into PATIENT_IDENTITY_XREF
    -- ------------------------------------------------------------------------
    MERGE INTO FIPSAR_PHI_HUB.PHI_CORE.PATIENT_IDENTITY_XREF tgt
    USING (
        SELECT DISTINCT
            UPPER(TRIM(FIRST_NAME)) || '|' || UPPER(TRIM(LAST_NAME)) || '|' || LOWER(TRIM(EMAIL)) AS IDENTITY_KEY,
            TRIM(FIRST_NAME) AS FIRST_NAME,
            TRIM(LAST_NAME) AS LAST_NAME,
            LOWER(TRIM(EMAIL)) AS EMAIL
        FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE
        WHERE NULLIF(TRIM(FIRST_NAME), '') IS NOT NULL
          AND NULLIF(TRIM(LAST_NAME), '') IS NOT NULL
          AND NULLIF(TRIM(EMAIL), '') IS NOT NULL
          AND NULLIF(TRIM(PHONE_NUMBER), '') IS NOT NULL
          AND COALESCE(
                TRY_TO_DATE(FILE_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(FILE_DATE, 'DD-MM-YYYY'),
                TRY_TO_DATE(FILE_DATE)
              ) IS NOT NULL
    ) src
    ON tgt.IDENTITY_KEY = src.IDENTITY_KEY
    WHEN NOT MATCHED THEN
        INSERT
        (
            MASTER_PATIENT_ID,
            IDENTITY_KEY,
            FIRST_NAME,
            LAST_NAME,
            EMAIL
        )
        VALUES
        (
            'FIP' || LPAD(FIPSAR_PHI_HUB.PHI_CORE.SEQ_MASTER_PATIENT_ID.NEXTVAL::VARCHAR, 10, '0'),
            src.IDENTITY_KEY,
            src.FIRST_NAME,
            src.LAST_NAME,
            src.EMAIL
        );

    v_new_ids := SQLROWCOUNT;

    -- ------------------------------------------------------------------------
    -- STEP 3: Insert only valid rows into PHI_PROSPECT_MASTER
    -- Keep current minimal delta guard: (MASTER_PATIENT_ID, FILE_DATE)
    -- ------------------------------------------------------------------------
    INSERT INTO FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER
    (
        MASTER_PATIENT_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        AGE,
        ADDRESS,
        APARTMENT_NO,
        CITY,
        STATE,
        ZIP_CODE,
        PATIENT_CONSENT,
        CHANNEL,
        SUBMISSION_TIMESTAMP,
        FILE_DATE
    )
    SELECT
        xref.MASTER_PATIENT_ID,
        TRIM(stg.FIRST_NAME) AS FIRST_NAME,
        TRIM(stg.LAST_NAME) AS LAST_NAME,
        LOWER(TRIM(stg.EMAIL)) AS EMAIL,
        TRIM(stg.PHONE_NUMBER) AS PHONE_NUMBER,
        TRY_CAST(stg.AGE AS INTEGER) AS AGE,
        stg.ADDRESS,
        stg.APARTMENT_NO,
        stg.CITY,
        UPPER(stg.STATE) AS STATE,
        stg.ZIP_CODE,
        CASE
            WHEN UPPER(TRIM(stg.PATIENT_CONSENT)) = 'TRUE' THEN TRUE
            ELSE FALSE
        END AS PATIENT_CONSENT,
        stg.CHANNEL,
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(stg.SUBMISSION_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP_NTZ(stg.SUBMISSION_TIMESTAMP, 'DD-MM-YYYY HH24:MI'),
            TRY_TO_TIMESTAMP_NTZ(stg.SUBMISSION_TIMESTAMP)
        ) AS SUBMISSION_TIMESTAMP,
        COALESCE(
            TRY_TO_DATE(stg.FILE_DATE, 'YYYY-MM-DD'),
            TRY_TO_DATE(stg.FILE_DATE, 'DD-MM-YYYY'),
            TRY_TO_DATE(stg.FILE_DATE)
        ) AS FILE_DATE
    FROM FIPSAR_PHI_HUB.STAGING.STG_PROSPECT_INTAKE stg
    INNER JOIN FIPSAR_PHI_HUB.PHI_CORE.PATIENT_IDENTITY_XREF xref
        ON UPPER(TRIM(stg.FIRST_NAME)) || '|' || UPPER(TRIM(stg.LAST_NAME)) || '|' || LOWER(TRIM(stg.EMAIL))
         = xref.IDENTITY_KEY
    WHERE NULLIF(TRIM(stg.FIRST_NAME), '') IS NOT NULL
      AND NULLIF(TRIM(stg.LAST_NAME), '') IS NOT NULL
      AND NULLIF(TRIM(stg.EMAIL), '') IS NOT NULL
      AND NULLIF(TRIM(stg.PHONE_NUMBER), '') IS NOT NULL
      AND COALESCE(
            TRY_TO_DATE(stg.FILE_DATE, 'YYYY-MM-DD'),
            TRY_TO_DATE(stg.FILE_DATE, 'DD-MM-YYYY'),
            TRY_TO_DATE(stg.FILE_DATE)
          ) IS NOT NULL
      AND NOT EXISTS
      (
          SELECT 1
          FROM FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER tgt
          WHERE tgt.MASTER_PATIENT_ID = xref.MASTER_PATIENT_ID
            AND tgt.FILE_DATE = COALESCE(
                                    TRY_TO_DATE(stg.FILE_DATE, 'YYYY-MM-DD'),
                                    TRY_TO_DATE(stg.FILE_DATE, 'DD-MM-YYYY'),
                                    TRY_TO_DATE(stg.FILE_DATE)
                                )
      );

    v_rows_loaded := SQLROWCOUNT;

    -- ------------------------------------------------------------------------
    -- STEP 4: Update run log
    -- ------------------------------------------------------------------------
    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_appended', :v_rows_loaded,
        'new_identities', :v_new_ids,
        'rejected_rows', :v_rejected_rows,
        'null_first_name', :v_null_first_name,
        'null_last_name', :v_null_last_name,
        'null_email', :v_null_email,
        'null_phone_number', :v_null_phone,
        'invalid_file_date', :v_invalid_file_date
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_PHI_PROSPECT failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_LOAD_PHI_PROSPECT failed'
        );
END;
$$;






CREATE OR REPLACE PROCEDURE FIPSAR_DW.BRONZE.SP_LOAD_BRONZE_PROSPECT()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_rows_loaded NUMBER DEFAULT 0;
    v_run_id      VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_BRONZE_PROSPECT', 'BRONZE', 'BRZ_PROSPECT_MASTER', 'STARTED'
    );

    INSERT INTO FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER
    (
        RECORD_ID, MASTER_PATIENT_ID, FIRST_NAME, LAST_NAME, EMAIL,
        PHONE_NUMBER, AGE, ADDRESS, APARTMENT_NO, CITY, STATE, ZIP_CODE,
        PATIENT_CONSENT, CHANNEL, SUBMISSION_TIMESTAMP, FILE_DATE,
        PHI_LOADED_AT, IS_ACTIVE, SOURCE_LAYER
    )
    SELECT
        p.RECORD_ID,
        p.MASTER_PATIENT_ID,
        p.FIRST_NAME,
        p.LAST_NAME,
        p.EMAIL,
        p.PHONE_NUMBER,
        p.AGE,
        p.ADDRESS,
        p.APARTMENT_NO,
        p.CITY,
        p.STATE,
        p.ZIP_CODE,
        p.PATIENT_CONSENT,
        p.CHANNEL,
        p.SUBMISSION_TIMESTAMP,
        p.FILE_DATE,
        p.PHI_LOADED_AT,
        p.IS_ACTIVE,
        'PHI_HUB'
    FROM FIPSAR_PHI_HUB.PHI_CORE.PHI_PROSPECT_MASTER p
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER b
        WHERE b.RECORD_ID = p.RECORD_ID
    );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_loaded', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_BRONZE_PROSPECT failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_LOAD_BRONZE_PROSPECT failed'
        );
END;
$$;

-- ============================================================================
-- VERIFICATION: Run these after executing this file
-- ============================================================================
-- SHOW DATABASES LIKE 'FIPSAR%';
-- SHOW SCHEMAS IN DATABASE FIPSAR_PHI_HUB;
-- SHOW SCHEMAS IN DATABASE FIPSAR_DW;
-- SHOW STAGES IN SCHEMA FIPSAR_PHI_HUB.STAGING;
-- DESC INTEGRATION FIPSAR_S3_INTEGRATION;   -- Copy values for AWS IAM Trust Policy
-- SELECT * FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG;

-- ============================================================================
-- END OF FILE 1: 01_ALIGNED_INFRASTRUCTURE_PHI_BRONZE.sql
-- NEXT: Run 02_ALIGNED_SILVER_GOLD.sql
-- ============================================================================



-- ============================================================================
-- SILVER SP: DQ checks, SCD2, incremental load
-- ============================================================================





CREATE OR REPLACE PROCEDURE FIPSAR_DW.SILVER.SP_LOAD_SILVER_PROSPECT()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id           VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded      NUMBER DEFAULT 0;
    v_rows_rejected    NUMBER DEFAULT 0;
    v_scd2_closed      NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_SILVER_PROSPECT', 'SILVER', 'SLV_PROSPECT_MASTER', 'STARTED'
    );

    -- ----------------------------------------------------------------------
    -- 1. LOG DQ REJECTIONS
    -- Rules:
    --   a) required fields missing / blank / N/A email
    --   b) duplicate RECORD_ID already in Silver
    --   c) duplicate RECORD_ID within Bronze batch (keep one)
    -- Prevent duplicate DQ logs across reruns
    -- ----------------------------------------------------------------------

    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    (
        TABLE_NAME, REJECTION_REASON, REJECTED_RECORD, REJECTED_AT
    )
    WITH bronze_ranked AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.RECORD_ID
                ORDER BY b.BRONZE_LOADED_AT, b.FILE_DATE, b.RECORD_ID
            ) AS RN_IN_BRONZE
        FROM FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER b
    ),
    rejected_rows AS (
        SELECT
            'SLV_PROSPECT_MASTER' AS TABLE_NAME,
            CASE
                WHEN NULLIF(TRIM(MASTER_PATIENT_ID), '') IS NULL THEN 'NULL_MASTER_PATIENT_ID'
                WHEN NULLIF(TRIM(FIRST_NAME), '') IS NULL THEN 'NULL_FIRST_NAME'
                WHEN NULLIF(TRIM(LAST_NAME), '') IS NULL THEN 'NULL_LAST_NAME'
                WHEN NULLIF(TRIM(EMAIL), '') IS NULL OR UPPER(TRIM(EMAIL)) = 'N/A' THEN 'NULL_EMAIL'
                WHEN RECORD_ID IN (SELECT RECORD_ID FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER) THEN 'DUPLICATE_RECORD_ID'
                WHEN RN_IN_BRONZE > 1 THEN 'DUPLICATE_RECORD_ID_IN_BRONZE'
            END AS REJECTION_REASON,
            OBJECT_CONSTRUCT(
                'RECORD_ID', RECORD_ID,
                'MASTER_PATIENT_ID', MASTER_PATIENT_ID,
                'FIRST_NAME', FIRST_NAME,
                'LAST_NAME', LAST_NAME,
                'EMAIL', EMAIL,
                'PHONE_NUMBER', PHONE_NUMBER,
                'FILE_DATE', FILE_DATE,
                'CHANNEL', CHANNEL,
                'STAGE', 'SILVER_LOAD'
            ) AS REJECTED_RECORD
        FROM bronze_ranked
        WHERE
            NULLIF(TRIM(MASTER_PATIENT_ID), '') IS NULL
            OR NULLIF(TRIM(FIRST_NAME), '') IS NULL
            OR NULLIF(TRIM(LAST_NAME), '') IS NULL
            OR NULLIF(TRIM(EMAIL), '') IS NULL
            OR UPPER(TRIM(EMAIL)) = 'N/A'
            OR RECORD_ID IN (SELECT RECORD_ID FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER)
            OR RN_IN_BRONZE > 1
    )
    SELECT
        r.TABLE_NAME,
        r.REJECTION_REASON,
        r.REJECTED_RECORD,
        CURRENT_TIMESTAMP()
    FROM rejected_rows r
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG d
        WHERE d.TABLE_NAME = r.TABLE_NAME
          AND d.REJECTION_REASON = r.REJECTION_REASON
          AND d.REJECTED_RECORD = r.REJECTED_RECORD
    );

    v_rows_rejected := SQLROWCOUNT;

    -- ----------------------------------------------------------------------
    -- 2. CLOSE CURRENT SCD2 ROWS ONLY WHEN CHANGED
    -- Use a valid bronze set:
    --   - required fields present
    --   - email not N/A
    --   - not already in silver by RECORD_ID
    --   - keep only one row per RECORD_ID from bronze batch
    -- ----------------------------------------------------------------------

    UPDATE FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER slv
    SET
        EFF_END_DATE = valid_src.FILE_DATE,
        IS_CURRENT = FALSE
    FROM
    (
        WITH bronze_ranked AS (
            SELECT
                b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY b.RECORD_ID
                    ORDER BY b.BRONZE_LOADED_AT, b.FILE_DATE, b.RECORD_ID
                ) AS RN_IN_BRONZE
            FROM FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER b
        )
        SELECT br.*
        FROM bronze_ranked br
        WHERE NULLIF(TRIM(br.MASTER_PATIENT_ID), '') IS NOT NULL
          AND NULLIF(TRIM(br.FIRST_NAME), '') IS NOT NULL
          AND NULLIF(TRIM(br.LAST_NAME), '') IS NOT NULL
          AND NULLIF(TRIM(br.EMAIL), '') IS NOT NULL
          AND UPPER(TRIM(br.EMAIL)) <> 'N/A'
          AND br.RN_IN_BRONZE = 1
          AND NOT EXISTS
          (
              SELECT 1
              FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER s
              WHERE s.RECORD_ID = br.RECORD_ID
          )
    ) valid_src
    WHERE slv.MASTER_PATIENT_ID = valid_src.MASTER_PATIENT_ID
      AND slv.IS_CURRENT = TRUE
      AND (
            COALESCE(slv.FIRST_NAME, '~')      <> COALESCE(valid_src.FIRST_NAME, '~')
         OR COALESCE(slv.LAST_NAME, '~')       <> COALESCE(valid_src.LAST_NAME, '~')
         OR COALESCE(slv.EMAIL, '~')           <> COALESCE(valid_src.EMAIL, '~')
         OR COALESCE(slv.PHONE_NUMBER, '~')    <> COALESCE(valid_src.PHONE_NUMBER, '~')
         OR COALESCE(slv.AGE, -1)              <> COALESCE(valid_src.AGE, -1)
         OR COALESCE(slv.ADDRESS, '~')         <> COALESCE(valid_src.ADDRESS, '~')
         OR COALESCE(slv.APARTMENT_NO, '~')    <> COALESCE(valid_src.APARTMENT_NO, '~')
         OR COALESCE(slv.CITY, '~')            <> COALESCE(valid_src.CITY, '~')
         OR COALESCE(slv.STATE, '~')           <> COALESCE(valid_src.STATE, '~')
         OR COALESCE(slv.ZIP_CODE, '~')        <> COALESCE(valid_src.ZIP_CODE, '~')
         OR COALESCE(slv.PATIENT_CONSENT, FALSE) <> COALESCE(valid_src.PATIENT_CONSENT, FALSE)
         OR COALESCE(slv.CHANNEL, '~')         <> COALESCE(valid_src.CHANNEL, '~')
         OR COALESCE(slv.IS_ACTIVE, FALSE)     <> COALESCE(valid_src.IS_ACTIVE, FALSE)
      );

    v_scd2_closed := SQLROWCOUNT;

    -- ----------------------------------------------------------------------
    -- 3. INSERT NEW CURRENT ROWS
    -- Insert:
    --   - new patient never seen before
    --   - or changed patient after old current row was closed
    -- Do not insert unchanged duplicates
    -- ----------------------------------------------------------------------

    INSERT INTO FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
    (
        RECORD_ID,
        MASTER_PATIENT_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        AGE,
        ADDRESS,
        APARTMENT_NO,
        CITY,
        STATE,
        ZIP_CODE,
        PATIENT_CONSENT,
        CHANNEL,
        SUBMISSION_TIMESTAMP,
        FILE_DATE,
        PHI_LOADED_AT,
        IS_ACTIVE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT,
        VERSION_NUM,
        DQ_PASSED
    )
    WITH bronze_ranked AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.RECORD_ID
                ORDER BY b.BRONZE_LOADED_AT, b.FILE_DATE, b.RECORD_ID
            ) AS RN_IN_BRONZE
        FROM FIPSAR_DW.BRONZE.BRZ_PROSPECT_MASTER b
    ),
    valid_bronze AS (
        SELECT br.*
        FROM bronze_ranked br
        WHERE NULLIF(TRIM(br.MASTER_PATIENT_ID), '') IS NOT NULL
          AND NULLIF(TRIM(br.FIRST_NAME), '') IS NOT NULL
          AND NULLIF(TRIM(br.LAST_NAME), '') IS NOT NULL
          AND NULLIF(TRIM(br.EMAIL), '') IS NOT NULL
          AND UPPER(TRIM(br.EMAIL)) <> 'N/A'
          AND br.RN_IN_BRONZE = 1
          AND NOT EXISTS
          (
              SELECT 1
              FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER s
              WHERE s.RECORD_ID = br.RECORD_ID
          )
    ),
    silver_curr AS (
        SELECT *
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        WHERE IS_CURRENT = TRUE
    ),
    max_ver AS (
        SELECT MASTER_PATIENT_ID, MAX(VERSION_NUM) AS MAX_VERSION_NUM
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        GROUP BY MASTER_PATIENT_ID
    )
    SELECT
        vb.RECORD_ID,
        vb.MASTER_PATIENT_ID,
        vb.FIRST_NAME,
        vb.LAST_NAME,
        LOWER(TRIM(vb.EMAIL)),
        vb.PHONE_NUMBER,
        vb.AGE,
        vb.ADDRESS,
        vb.APARTMENT_NO,
        vb.CITY,
        vb.STATE,
        vb.ZIP_CODE,
        vb.PATIENT_CONSENT,
        vb.CHANNEL,
        vb.SUBMISSION_TIMESTAMP,
        vb.FILE_DATE,
        vb.PHI_LOADED_AT,
        vb.IS_ACTIVE,
        vb.FILE_DATE AS EFF_START_DATE,
        TO_DATE('9999-12-31') AS EFF_END_DATE,
        TRUE AS IS_CURRENT,
        COALESCE(mv.MAX_VERSION_NUM, 0) + 1 AS VERSION_NUM,
        TRUE AS DQ_PASSED
    FROM valid_bronze vb
    LEFT JOIN silver_curr sc
        ON sc.MASTER_PATIENT_ID = vb.MASTER_PATIENT_ID
    LEFT JOIN max_ver mv
        ON mv.MASTER_PATIENT_ID = vb.MASTER_PATIENT_ID
    WHERE
        sc.MASTER_PATIENT_ID IS NULL
        OR (
            COALESCE(sc.FIRST_NAME, '~')      <> COALESCE(vb.FIRST_NAME, '~')
         OR COALESCE(sc.LAST_NAME, '~')       <> COALESCE(vb.LAST_NAME, '~')
         OR COALESCE(sc.EMAIL, '~')           <> COALESCE(vb.EMAIL, '~')
         OR COALESCE(sc.PHONE_NUMBER, '~')    <> COALESCE(vb.PHONE_NUMBER, '~')
         OR COALESCE(sc.AGE, -1)              <> COALESCE(vb.AGE, -1)
         OR COALESCE(sc.ADDRESS, '~')         <> COALESCE(vb.ADDRESS, '~')
         OR COALESCE(sc.APARTMENT_NO, '~')    <> COALESCE(vb.APARTMENT_NO, '~')
         OR COALESCE(sc.CITY, '~')            <> COALESCE(vb.CITY, '~')
         OR COALESCE(sc.STATE, '~')           <> COALESCE(vb.STATE, '~')
         OR COALESCE(sc.ZIP_CODE, '~')        <> COALESCE(vb.ZIP_CODE, '~')
         OR COALESCE(sc.PATIENT_CONSENT, FALSE) <> COALESCE(vb.PATIENT_CONSENT, FALSE)
         OR COALESCE(sc.CHANNEL, '~')         <> COALESCE(vb.CHANNEL, '~')
         OR COALESCE(sc.IS_ACTIVE, FALSE)     <> COALESCE(vb.IS_ACTIVE, FALSE)
        );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = :v_rows_rejected,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_loaded', :v_rows_loaded,
        'rows_rejected', :v_rows_rejected,
        'scd2_closed', :v_scd2_closed
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_SILVER_PROSPECT failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_LOAD_SILVER_PROSPECT failed'
        );
END;
$$;




-- ============================================================================
-- GOLD: STORED PROCEDURES
-- ============================================================================



CREATE OR REPLACE PROCEDURE FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_CHANNEL()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_GOLD_DIM_CHANNEL', 'GOLD', 'DIM_CHANNEL', 'STARTED'
    );

    MERGE INTO FIPSAR_DW.GOLD.DIM_CHANNEL tgt
    USING (
        SELECT DISTINCT
            CHANNEL AS CHANNEL_NAME,
            CASE
                WHEN CHANNEL IN ('Instagram', 'Facebook', 'Website', 'Campaign App', 'Referral', 'Survey') THEN 'Digital'
                ELSE 'Other'
            END AS CHANNEL_CATEGORY
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        WHERE DQ_PASSED = TRUE
          AND NULLIF(TRIM(CHANNEL), '') IS NOT NULL
    ) src
    ON tgt.CHANNEL_NAME = src.CHANNEL_NAME
    WHEN MATCHED THEN UPDATE SET
        tgt.CHANNEL_CATEGORY = src.CHANNEL_CATEGORY
    WHEN NOT MATCHED THEN
        INSERT (CHANNEL_NAME, CHANNEL_CATEGORY)
        VALUES (src.CHANNEL_NAME, src.CHANNEL_CATEGORY);

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_loaded', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_GOLD_DIM_CHANNEL failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_LOAD_GOLD_DIM_CHANNEL failed'
        );
END;
$$;






CREATE OR REPLACE PROCEDURE FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_DATE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_GOLD_DIM_DATE', 'GOLD', 'DIM_DATE', 'STARTED'
    );

    INSERT INTO FIPSAR_DW.GOLD.DIM_DATE
    (
        DATE_KEY, FULL_DATE, YEAR, QUARTER, MONTH, MONTH_NAME,
        WEEK_OF_YEAR, DAY_OF_MONTH, DAY_OF_WEEK, DAY_NAME, IS_WEEKEND
    )
    SELECT
        TO_NUMBER(TO_CHAR(d.FILE_DATE, 'YYYYMMDD')) AS DATE_KEY,
        d.FILE_DATE AS FULL_DATE,
        YEAR(d.FILE_DATE) AS YEAR,
        QUARTER(d.FILE_DATE) AS QUARTER,
        MONTH(d.FILE_DATE) AS MONTH,
        TO_CHAR(d.FILE_DATE, 'MMMM') AS MONTH_NAME,
        WEEKOFYEAR(d.FILE_DATE) AS WEEK_OF_YEAR,
        DAY(d.FILE_DATE) AS DAY_OF_MONTH,
        DAYOFWEEK(d.FILE_DATE) AS DAY_OF_WEEK,
        TO_CHAR(d.FILE_DATE, 'DY') AS DAY_NAME,
        CASE WHEN DAYOFWEEK(d.FILE_DATE) IN (0,6) THEN TRUE ELSE FALSE END AS IS_WEEKEND
    FROM (
        SELECT DISTINCT FILE_DATE
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        WHERE DQ_PASSED = TRUE
          AND FILE_DATE IS NOT NULL
    ) d
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM FIPSAR_DW.GOLD.DIM_DATE tgt
        WHERE tgt.FULL_DATE = d.FILE_DATE
    );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_loaded', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_GOLD_DIM_DATE failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_LOAD_GOLD_DIM_DATE failed'
        );
END;
$$;





CREATE OR REPLACE PROCEDURE FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_GEOGRAPHY()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_GOLD_DIM_GEOGRAPHY', 'GOLD', 'DIM_GEOGRAPHY', 'STARTED'
    );

    MERGE INTO FIPSAR_DW.GOLD.DIM_GEOGRAPHY tgt
    USING (
        SELECT DISTINCT
            CITY,
            STATE,
            ZIP_CODE,
            CASE
                WHEN STATE IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
                WHEN STATE IN ('IL','IN','IA','KS','MI','MN','MO','NE','ND','OH','SD','WI') THEN 'Midwest'
                WHEN STATE IN ('AL','AR','DE','FL','GA','KY','LA','MD','MS','NC','OK','SC','TN','TX','VA','WV','DC') THEN 'South'
                WHEN STATE IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
                ELSE 'Other'
            END AS REGION
        FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
        WHERE DQ_PASSED = TRUE
          AND NULLIF(TRIM(CITY), '') IS NOT NULL
          AND NULLIF(TRIM(STATE), '') IS NOT NULL
          AND NULLIF(TRIM(ZIP_CODE), '') IS NOT NULL
    ) src
    ON tgt.CITY = src.CITY
   AND tgt.STATE = src.STATE
   AND tgt.ZIP_CODE = src.ZIP_CODE
    WHEN MATCHED THEN UPDATE SET
        tgt.REGION = src.REGION
    WHEN NOT MATCHED THEN
        INSERT (CITY, STATE, ZIP_CODE, REGION)
        VALUES (src.CITY, src.STATE, src.ZIP_CODE, src.REGION);

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_loaded', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_GOLD_DIM_GEOGRAPHY failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_LOAD_GOLD_DIM_GEOGRAPHY failed'
        );
END;
$$;



CREATE OR REPLACE PROCEDURE FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_PROSPECT()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_GOLD_DIM_PROSPECT', 'GOLD', 'DIM_PROSPECT', 'STARTED'
    );

    MERGE INTO FIPSAR_DW.GOLD.DIM_PROSPECT tgt
    USING (
        WITH current_rows AS (
            SELECT *
            FROM (
                SELECT
                    s.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.MASTER_PATIENT_ID
                        ORDER BY s.EFF_START_DATE DESC,
                                 s.SILVER_LOADED_AT DESC,
                                 s.SLV_KEY DESC
                    ) AS RN
                FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER s
                WHERE s.DQ_PASSED = TRUE
                  AND s.IS_CURRENT = TRUE
            )
            WHERE RN = 1
        ),
        intake_stats AS (
            SELECT
                MASTER_PATIENT_ID,
                MIN(FILE_DATE) AS FIRST_INTAKE_DATE,
                MAX(FILE_DATE) AS LAST_INTAKE_DATE,
                COUNT(*) AS INTAKE_COUNT
            FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
            WHERE DQ_PASSED = TRUE
            GROUP BY MASTER_PATIENT_ID
        ),
        primary_channel AS (
            SELECT
                MASTER_PATIENT_ID,
                CHANNEL AS PRIMARY_CHANNEL
            FROM (
                SELECT
                    MASTER_PATIENT_ID,
                    CHANNEL,
                    SUBMISSION_TIMESTAMP,
                    ROW_NUMBER() OVER (
                        PARTITION BY MASTER_PATIENT_ID
                        ORDER BY SUBMISSION_TIMESTAMP
                    ) AS RN
                FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER
                WHERE DQ_PASSED = TRUE
            )
            WHERE RN = 1
        )
        SELECT
            c.MASTER_PATIENT_ID,
            c.FIRST_NAME,
            c.LAST_NAME,
            c.EMAIL,
            c.PHONE_NUMBER,
            c.AGE,
            CASE
                WHEN c.AGE < 18 THEN 'Under 18'
                WHEN c.AGE BETWEEN 18 AND 25 THEN '18-25'
                WHEN c.AGE BETWEEN 26 AND 35 THEN '26-35'
                WHEN c.AGE BETWEEN 36 AND 45 THEN '36-45'
                WHEN c.AGE BETWEEN 46 AND 55 THEN '46-55'
                WHEN c.AGE BETWEEN 56 AND 65 THEN '56-65'
                WHEN c.AGE > 65 THEN '65+'
                ELSE 'Unknown'
            END AS AGE_GROUP,
            c.CITY,
            c.STATE,
            c.ZIP_CODE,
            c.PATIENT_CONSENT,
            s.FIRST_INTAKE_DATE,
            s.LAST_INTAKE_DATE,
            s.INTAKE_COUNT,
            pc.PRIMARY_CHANNEL,
            c.IS_ACTIVE
        FROM current_rows c
        INNER JOIN intake_stats s
            ON c.MASTER_PATIENT_ID = s.MASTER_PATIENT_ID
        LEFT JOIN primary_channel pc
            ON c.MASTER_PATIENT_ID = pc.MASTER_PATIENT_ID
    ) src
    ON tgt.MASTER_PATIENT_ID = src.MASTER_PATIENT_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.FIRST_NAME = src.FIRST_NAME,
        tgt.LAST_NAME = src.LAST_NAME,
        tgt.EMAIL = src.EMAIL,
        tgt.PHONE_NUMBER = src.PHONE_NUMBER,
        tgt.AGE = src.AGE,
        tgt.AGE_GROUP = src.AGE_GROUP,
        tgt.CITY = src.CITY,
        tgt.STATE = src.STATE,
        tgt.ZIP_CODE = src.ZIP_CODE,
        tgt.PATIENT_CONSENT = src.PATIENT_CONSENT,
        tgt.FIRST_INTAKE_DATE = src.FIRST_INTAKE_DATE,
        tgt.LAST_INTAKE_DATE = src.LAST_INTAKE_DATE,
        tgt.INTAKE_COUNT = src.INTAKE_COUNT,
        tgt.PRIMARY_CHANNEL = src.PRIMARY_CHANNEL,
        tgt.IS_ACTIVE = src.IS_ACTIVE,
        tgt.UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT
        (
            MASTER_PATIENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER,
            AGE, AGE_GROUP, CITY, STATE, ZIP_CODE, PATIENT_CONSENT,
            FIRST_INTAKE_DATE, LAST_INTAKE_DATE, INTAKE_COUNT,
            PRIMARY_CHANNEL, IS_ACTIVE
        )
        VALUES
        (
            src.MASTER_PATIENT_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.PHONE_NUMBER,
            src.AGE, src.AGE_GROUP, src.CITY, src.STATE, src.ZIP_CODE, src.PATIENT_CONSENT,
            src.FIRST_INTAKE_DATE, src.LAST_INTAKE_DATE, src.INTAKE_COUNT,
            src.PRIMARY_CHANNEL, src.IS_ACTIVE
        );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_loaded', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_GOLD_DIM_PROSPECT failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_LOAD_GOLD_DIM_PROSPECT failed'
        );
END;
$$;








CREATE OR REPLACE PROCEDURE FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_INTAKE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_rows_loaded NUMBER DEFAULT 0;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS
    )
    VALUES
    (
        :v_run_id, 'SP_LOAD_GOLD_FACT_INTAKE', 'GOLD', 'FACT_PROSPECT_INTAKE', 'STARTED'
    );

    INSERT INTO FIPSAR_DW.GOLD.FACT_PROSPECT_INTAKE
    (
        RECORD_ID,
        MASTER_PATIENT_ID,
        PROSPECT_KEY,
        CHANNEL_KEY,
        DATE_KEY,
        GEO_KEY,
        SUBMISSION_TIMESTAMP,
        FILE_DATE,
        AGE,
        CONSENT_FLAG
    )
    SELECT
        slv.RECORD_ID,
        slv.MASTER_PATIENT_ID,
        dp.PROSPECT_KEY,
        dc.CHANNEL_KEY,
        dd.DATE_KEY,
        dg.GEO_KEY,
        slv.SUBMISSION_TIMESTAMP,
        slv.FILE_DATE,
        slv.AGE,
        CASE WHEN slv.PATIENT_CONSENT = TRUE THEN 1 ELSE 0 END AS CONSENT_FLAG
    FROM FIPSAR_DW.SILVER.SLV_PROSPECT_MASTER slv
    LEFT JOIN FIPSAR_DW.GOLD.DIM_PROSPECT dp
        ON slv.MASTER_PATIENT_ID = dp.MASTER_PATIENT_ID
    LEFT JOIN FIPSAR_DW.GOLD.DIM_CHANNEL dc
        ON slv.CHANNEL = dc.CHANNEL_NAME
    LEFT JOIN FIPSAR_DW.GOLD.DIM_DATE dd
        ON slv.FILE_DATE = dd.FULL_DATE
    LEFT JOIN FIPSAR_DW.GOLD.DIM_GEOGRAPHY dg
        ON slv.CITY = dg.CITY
       AND slv.STATE = dg.STATE
       AND slv.ZIP_CODE = dg.ZIP_CODE
    WHERE slv.DQ_PASSED = TRUE
      AND NOT EXISTS
      (
          SELECT 1
          FROM FIPSAR_DW.GOLD.FACT_PROSPECT_INTAKE f
          WHERE f.RECORD_ID = slv.RECORD_ID
      );

    v_rows_loaded := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows_loaded,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_loaded', :v_rows_loaded
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_LOAD_GOLD_FACT_INTAKE failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_LOAD_GOLD_FACT_INTAKE failed'
        );
END;
$$;





  
CREATE OR REPLACE PROCEDURE FIPSAR_DW.GOLD.SP_EXPORT_SFMC_OUTBOUND()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_rows        INTEGER DEFAULT 0;
    v_run_id      VARCHAR DEFAULT UUID_STRING();
    v_timestamp   VARCHAR;
    v_target_file VARCHAR;
    v_copy_sql    VARCHAR;
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID,
        PIPELINE_NAME,
        LAYER,
        TABLE_NAME,
        STATUS
    )
    VALUES
    (
        :v_run_id,
        'SP_EXPORT_SFMC_OUTBOUND',
        'GOLD',
        'VW_SFMC_PROSPECT_OUTBOUND',
        'STARTED'
    );

    v_timestamp := TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MI');
    v_target_file := '@FIPSAR_PHI_HUB.STAGING.STG_SFMC_OUTBOUND/Prospect_c_delta_' || v_timestamp || '.csv';

    v_copy_sql := '
        COPY INTO ' || v_target_file || '
        FROM (
            SELECT *
            FROM FIPSAR_DW.GOLD.VW_SFMC_PROSPECT_OUTBOUND
        )
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = ''"''
            COMPRESSION = NONE
        )
        HEADER = TRUE
        SINGLE = TRUE
        MAX_FILE_SIZE = 50000000
    ';

    EXECUTE IMMEDIATE v_copy_sql;

    SELECT COUNT(*)
    INTO :v_rows
    FROM FIPSAR_DW.GOLD.VW_SFMC_PROSPECT_OUTBOUND;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET
        STATUS = 'SUCCESS',
        ROWS_LOADED = :v_rows,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'run_id', :v_run_id,
        'rows_exported', :v_rows,
        'file_name', 'Prospect_c_delta_' || :v_timestamp || '.csv'
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET
            STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_EXPORT_SFMC_OUTBOUND failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status', 'FAILED',
            'run_id', :v_run_id,
            'error', 'SP_EXPORT_SFMC_OUTBOUND failed'
        );
END;
$$;






CREATE OR REPLACE PROCEDURE FIPSAR_AUDIT.PIPELINE_AUDIT.SP_SEND_SILVER_DQ_STATUS_EMAIL()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_phi_status       VARCHAR DEFAULT 'NOT FOUND';
    v_phi_rows         NUMBER  DEFAULT 0;
    v_phi_error        VARCHAR DEFAULT '';
    v_phi_started      TIMESTAMP_NTZ;
    v_phi_completed    TIMESTAMP_NTZ;

    v_bronze_status    VARCHAR DEFAULT 'NOT FOUND';
    v_bronze_rows      NUMBER  DEFAULT 0;
    v_bronze_error     VARCHAR DEFAULT '';
    v_bronze_started   TIMESTAMP_NTZ;
    v_bronze_completed TIMESTAMP_NTZ;

    v_silver_status    VARCHAR DEFAULT 'NOT FOUND';
    v_silver_rows      NUMBER  DEFAULT 0;
    v_silver_error     VARCHAR DEFAULT '';
    v_silver_started   TIMESTAMP_NTZ;
    v_silver_completed TIMESTAMP_NTZ;

    v_dq_count         NUMBER  DEFAULT 0;
    v_dq_summary       VARCHAR DEFAULT 'No DQ rejections found.';
    v_email_body       VARCHAR;
BEGIN
    SELECT
        COALESCE(MAX(STATUS), 'NOT FOUND'),
        COALESCE(MAX(ROWS_LOADED), 0),
        COALESCE(MAX(ERROR_MESSAGE), ''),
        MAX(STARTED_AT),
        MAX(COMPLETED_AT)
    INTO
        :v_phi_status, :v_phi_rows, :v_phi_error, :v_phi_started, :v_phi_completed
    FROM (
        SELECT *
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        WHERE PIPELINE_NAME = 'SP_LOAD_PHI_PROSPECT'
        ORDER BY STARTED_AT DESC
        LIMIT 1
    );

    SELECT
        COALESCE(MAX(STATUS), 'NOT FOUND'),
        COALESCE(MAX(ROWS_LOADED), 0),
        COALESCE(MAX(ERROR_MESSAGE), ''),
        MAX(STARTED_AT),
        MAX(COMPLETED_AT)
    INTO
        :v_bronze_status, :v_bronze_rows, :v_bronze_error, :v_bronze_started, :v_bronze_completed
    FROM (
        SELECT *
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        WHERE PIPELINE_NAME = 'SP_LOAD_BRONZE_PROSPECT'
        ORDER BY STARTED_AT DESC
        LIMIT 1
    );

    SELECT
        COALESCE(MAX(STATUS), 'NOT FOUND'),
        COALESCE(MAX(ROWS_LOADED), 0),
        COALESCE(MAX(ERROR_MESSAGE), ''),
        MAX(STARTED_AT),
        MAX(COMPLETED_AT)
    INTO
        :v_silver_status, :v_silver_rows, :v_silver_error, :v_silver_started, :v_silver_completed
    FROM (
        SELECT *
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        WHERE PIPELINE_NAME = 'SP_LOAD_SILVER_PROSPECT'
        ORDER BY STARTED_AT DESC
        LIMIT 1
    );

    SELECT COUNT(*)
    INTO :v_dq_count
    FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
    WHERE REJECTED_AT >= COALESCE(:v_silver_started, DATEADD(HOUR,-2,CURRENT_TIMESTAMP()));

    SELECT COALESCE(
        LISTAGG(COALESCE(REJECTION_REASON,'UNKNOWN') || ' = ' || CNT, ' | '),
        'No DQ rejections.'
    )
    INTO :v_dq_summary
    FROM (
        SELECT REJECTION_REASON, COUNT(*) AS CNT
        FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        WHERE REJECTED_AT >= COALESCE(:v_silver_started, DATEADD(HOUR,-2,CURRENT_TIMESTAMP()))
        GROUP BY REJECTION_REASON
        ORDER BY CNT DESC
    );

    v_email_body :=
          'FIPSAR Pipeline Mid-Run Status Update' || CHAR(10) || CHAR(10)
        || '1) PHI Load:    ' || COALESCE(:v_phi_status,'NOT FOUND')
        || ' | Rows: '        || COALESCE(TO_VARCHAR(:v_phi_rows),'0')
        || ' | Completed: '   || COALESCE(TO_VARCHAR(:v_phi_completed),'N/A') || CHAR(10)
        || '2) Bronze Load: ' || COALESCE(:v_bronze_status,'NOT FOUND')
        || ' | Rows: '        || COALESCE(TO_VARCHAR(:v_bronze_rows),'0')
        || ' | Completed: '   || COALESCE(TO_VARCHAR(:v_bronze_completed),'N/A') || CHAR(10)
        || '3) Silver Load: ' || COALESCE(:v_silver_status,'NOT FOUND')
        || ' | Rows: '        || COALESCE(TO_VARCHAR(:v_silver_rows),'0')
        || ' | Completed: '   || COALESCE(TO_VARCHAR(:v_silver_completed),'N/A') || CHAR(10) || CHAR(10)
        || '4) DQ Rejections since Silver start: ' || COALESCE(TO_VARCHAR(:v_dq_count),'0') || CHAR(10)
        || '   Breakdown: '   || COALESCE(:v_dq_summary,'No DQ rejections found.') || CHAR(10) || CHAR(10)
        || 'Tables: FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG | DQ_REJECTION_LOG';

    CALL SYSTEM$SEND_EMAIL(
        'FIPSAR_EMAIL_NOTIFICATION',
        'rosnow1201@gmail.com',
        'FIPSAR Pipeline Status after Silver Load',
        :v_email_body
    );

    RETURN OBJECT_CONSTRUCT('status','SUCCESS','dq_reject_count',:v_dq_count);

EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'status','FAILED',
            'error','SP_SEND_SILVER_DQ_STATUS_EMAIL failed'
        );
END;
$$;







-- =====================================================================================
-- PART 5: END-TO-END ORCHESTRATOR
-- =====================================================================================

CREATE OR REPLACE PROCEDURE FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_RUN_SFMC_EVENTS_PIPELINE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    (
        RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS, STARTED_AT
    )
    VALUES
    (
        :v_run_id, 'SP_RUN_SFMC_EVENTS_PIPELINE', 'END_TO_END', 'SFMC_EVENTS_PIPELINE', 'STARTED', CURRENT_TIMESTAMP()
    );

    -- RAW
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SENT();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_OPENS();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_CLICKS();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_BOUNCES();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBES();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_SPAM();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_METADATA();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOB_DE_DETAIL();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_UNSUBSCRIBE_DE_DETAIL();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_C_HISTORY();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_PROSPECT_JOURNEY_DETAILS();
    CALL FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_LOAD_RAW_SFMC_JOURNEY_ACTIVITY_GENERIC();

    -- GOLD
    CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_ENGAGEMENT_TYPE();
    CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_DIM_SFMC_JOB();
    CALL FIPSAR_DW.GOLD.SP_LOG_SFMC_SUPPRESSIONS();
    CALL FIPSAR_DW.GOLD.SP_LOAD_GOLD_FACT_SFMC_ENGAGEMENT();

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS = 'SUCCESS',
        ROWS_LOADED = 0,
        ROWS_REJECTED = 0,
        ERROR_MESSAGE = NULL,
        COMPLETED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :v_run_id;

    RETURN OBJECT_CONSTRUCT(
        'status','SUCCESS',
        'run_id',:v_run_id,
        'pipeline','SFMC_EVENTS_PIPELINE'
    );

EXCEPTION
    WHEN OTHER THEN
        UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        SET STATUS = 'FAILED',
            ERROR_MESSAGE = 'SP_RUN_SFMC_EVENTS_PIPELINE failed',
            COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE RUN_ID = :v_run_id;

        RETURN OBJECT_CONSTRUCT(
            'status','FAILED',
            'run_id',:v_run_id,
            'error','SP_RUN_SFMC_EVENTS_PIPELINE failed'
        );
END;
$$;






-- ============================================================================
-- SECTION D: NEW SP — SP_PROCESS_SFMC_SUPPRESSION (ENHANCEMENT-D)
-- Reads JOURNEY_DETAILS for SUPPRESSION_FLAG='True'.
-- Updates FACT_SFMC_ENGAGEMENT IS_SUPPRESSED=TRUE for those prospects.
-- Writes 120 rows to DQ_REJECTION_LOG with reason 'SUPPRESSED_PROSPECT'.
-- Call this after SP_LOAD_GOLD_FACT_ENGAGEMENT.
-- ============================================================================
CREATE OR REPLACE PROCEDURE FIPSAR_SFMC_EVENTS.RAW_EVENTS.SP_PROCESS_SFMC_SUPPRESSION()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_updated INTEGER DEFAULT 0;
    v_logged  INTEGER DEFAULT 0;
    v_run_id  VARCHAR DEFAULT UUID_STRING();
BEGIN
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
        (RUN_ID, PIPELINE_NAME, LAYER, TABLE_NAME, STATUS)
    VALUES (:v_run_id, 'SP_PROCESS_SFMC_SUPPRESSION', 'GOLD', 'FACT_SFMC_ENGAGEMENT', 'STARTED');

    -- Mark suppressed prospects in FACT
    UPDATE FIPSAR_DW.GOLD.FACT_SFMC_ENGAGEMENT fe
    SET fe.IS_SUPPRESSED      = TRUE,
        fe.SUPPRESSION_REASON = 'Prospect unsubscribed — journey suppression flag active'
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
    WHERE fe.SUBSCRIBER_KEY = jd.PROSPECT_ID
      AND jd.SUPPRESSION_FLAG = 'True'
      AND COALESCE(fe.IS_SUPPRESSED, FALSE) = FALSE;

    v_updated := SQLROWCOUNT;

    -- Log suppressed prospects to DQ_REJECTION_LOG
    INSERT INTO FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG
        (TABLE_NAME, REJECTION_REASON, REJECTED_RECORD)
    SELECT DISTINCT
        'FACT_SFMC_ENGAGEMENT',
        'SUPPRESSED_PROSPECT',
        OBJECT_CONSTRUCT(
            'PROSPECT_ID',              jd.PROSPECT_ID,
            'SUPPRESSION_FLAG',         jd.SUPPRESSION_FLAG,
            'WELCOME_JOURNEY_COMPLETE', jd.WELCOME_JOURNEY_COMPLETE,
            'NURTURE_JOURNEY_COMPLETE', jd.NURTURE_JOURNEY_COMPLETE,
            'STAGE',                    'SFMC_SUPPRESSION_DETECTION'
        )
    FROM FIPSAR_SFMC_EVENTS.RAW_EVENTS.RAW_SFMC_PROSPECT_JOURNEY_DETAILS jd
    WHERE jd.SUPPRESSION_FLAG = 'True'
      AND NOT EXISTS (
          SELECT 1 FROM FIPSAR_AUDIT.PIPELINE_AUDIT.DQ_REJECTION_LOG dq
          WHERE dq.REJECTION_REASON = 'SUPPRESSED_PROSPECT'
            AND TRY_TO_VARCHAR(dq.REJECTED_RECORD:PROSPECT_ID) = jd.PROSPECT_ID
      );

    v_logged := SQLROWCOUNT;

    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS='SUCCESS', ROWS_LOADED=:v_updated, ROWS_REJECTED=:v_logged,
        COMPLETED_AT=CURRENT_TIMESTAMP()
    WHERE RUN_ID=:v_run_id;

    RETURN OBJECT_CONSTRUCT('status','SUCCESS','fact_rows_updated',:v_updated,'dq_rows_logged',:v_logged);
EXCEPTION WHEN OTHER THEN
    UPDATE FIPSAR_AUDIT.PIPELINE_AUDIT.PIPELINE_RUN_LOG
    SET STATUS='FAILED', ERROR_MESSAGE=SQLERRM, COMPLETED_AT=CURRENT_TIMESTAMP()
    WHERE RUN_ID=:v_run_id;
    RETURN OBJECT_CONSTRUCT('status','FAILED','error',SQLERRM);
END;
$$;