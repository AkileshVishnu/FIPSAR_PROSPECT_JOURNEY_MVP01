# ── ECS Task Execution Role ───────────────────────────────────────────────────
# Used by the ECS agent to pull images from ECR and inject secrets at startup.

resource "aws_iam_role" "ecs_execution" {
  name        = "${local.name}-ecs-execution-role"
  description = "ECS task execution role — ECR pull + Secrets Manager read"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "ECSTasksAssumeRole"
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

# AWS-managed policy covers ECR pull + CloudWatch Logs
resource "aws_iam_role_policy_attachment" "ecs_execution_managed" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Allow the execution role to read our specific secrets
resource "aws_iam_role_policy" "ecs_execution_secrets" {
  name = "${local.name}-secrets-read"
  role = aws_iam_role.ecs_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadSecrets"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = [
          aws_secretsmanager_secret.snowflake.arn,
          aws_secretsmanager_secret.openai.arn,
          aws_secretsmanager_secret.email.arn,
        ]
      }
    ]
  })
}

# ── ECS Task Role ─────────────────────────────────────────────────────────────
# Assumed by the running container itself (for AWS SDK calls from app code).
# Currently empty — add S3 / SES / etc. policies here as needed.

resource "aws_iam_role" "ecs_task" {
  name        = "${local.name}-ecs-task-role"
  description = "Runtime role assumed by the FIPSAR Streamlit container"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "ECSTasksAssumeRole"
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

# Example: uncomment to allow app to send email via SES instead of SMTP
# resource "aws_iam_role_policy" "ecs_task_ses" {
#   name = "${local.name}-ses"
#   role = aws_iam_role.ecs_task.id
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [{
#       Effect   = "Allow"
#       Action   = ["ses:SendEmail", "ses:SendRawEmail"]
#       Resource = "*"
#     }]
#   })
# }
