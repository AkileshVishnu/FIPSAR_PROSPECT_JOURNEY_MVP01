# ── General ───────────────────────────────────────────────────────────────────

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Short project prefix used in all resource names"
  type        = string
  default     = "fipsar"
}

variable "environment" {
  description = "Deployment environment (prod, staging, dev)"
  type        = string
  default     = "prod"
}

# ── Networking ─────────────────────────────────────────────────────────────────

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

# ── Container / ECS ───────────────────────────────────────────────────────────

variable "task_cpu" {
  description = "CPU units for the Fargate task (1024 = 1 vCPU)"
  type        = number
  default     = 1024
}

variable "task_memory" {
  description = "Memory in MiB for the Fargate task"
  type        = number
  default     = 2048
}

variable "desired_count" {
  description = "Number of running ECS task instances"
  type        = number
  default     = 1
}

variable "container_image_tag" {
  description = "Docker image tag to deploy (updated by CI/CD pipeline)"
  type        = string
  default     = "latest"
}

# ── Snowflake secrets (sensitive — pass via terraform.tfvars or env vars) ──────

variable "snowflake_account" {
  description = "Snowflake account identifier (e.g. xy12345.us-east-1)"
  type        = string
  sensitive   = true
}

variable "snowflake_user" {
  description = "Snowflake login username"
  type        = string
  sensitive   = true
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  type        = string
  default     = "COMPUTE_WH"
}

variable "snowflake_database" {
  description = "Snowflake database name"
  type        = string
  default     = "FIPSAR_DW"
}

variable "snowflake_schema" {
  description = "Snowflake schema name"
  type        = string
  default     = "GOLD"
}

variable "snowflake_role" {
  description = "Snowflake role name"
  type        = string
  default     = "SYSADMIN"
}

# ── OpenAI ────────────────────────────────────────────────────────────────────

variable "openai_api_key" {
  description = "OpenAI API key"
  type        = string
  sensitive   = true
}

variable "openai_model" {
  description = "OpenAI model identifier"
  type        = string
  default     = "gpt-4o"
}

# ── Email (optional) ──────────────────────────────────────────────────────────

variable "email_smtp_host" {
  description = "SMTP host for outbound email"
  type        = string
  default     = "smtp.gmail.com"
}

variable "email_smtp_port" {
  description = "SMTP port"
  type        = number
  default     = 587
}

variable "email_smtp_user" {
  description = "SMTP username / sender address"
  type        = string
  default     = ""
}

variable "email_smtp_password" {
  description = "SMTP password or app-specific password"
  type        = string
  sensitive   = true
  default     = ""
}

variable "email_to" {
  description = "Default recipient address for FREL Agent reports"
  type        = string
  default     = ""
}
