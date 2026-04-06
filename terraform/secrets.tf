# ── Snowflake credentials (stored as a JSON blob) ─────────────────────────────
#
# ECS task pulls individual keys via the JSON key syntax:
#   "${secret_arn}:KEY_NAME::"

resource "aws_secretsmanager_secret" "snowflake" {
  name                    = "${local.name}/snowflake"
  description             = "Snowflake connection credentials for ${local.name}"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "snowflake" {
  secret_id = aws_secretsmanager_secret.snowflake.id
  secret_string = jsonencode({
    SNOWFLAKE_ACCOUNT  = var.snowflake_account
    SNOWFLAKE_USER     = var.snowflake_user
    SNOWFLAKE_PASSWORD = var.snowflake_password
  })
}

# ── OpenAI API key (plain string secret) ─────────────────────────────────────

resource "aws_secretsmanager_secret" "openai" {
  name                    = "${local.name}/openai-api-key"
  description             = "OpenAI API key for ${local.name}"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "openai" {
  secret_id     = aws_secretsmanager_secret.openai.id
  secret_string = var.openai_api_key
}

# ── Email SMTP password (plain string secret) ─────────────────────────────────

resource "aws_secretsmanager_secret" "email" {
  name                    = "${local.name}/email-smtp-password"
  description             = "SMTP password for FREL Agent email reports"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "email" {
  secret_id     = aws_secretsmanager_secret.email.id
  secret_string = var.email_smtp_password != "" ? var.email_smtp_password : "NOT_CONFIGURED"
}
