#!/usr/bin/env bash
# deploy.sh — Full deploy: Terraform apply → Docker build/push → ECS force-deploy
#
# Usage:
#   cd terraform/
#   cp terraform.tfvars.example terraform.tfvars   # fill in secrets
#   ./deploy.sh [init|plan|apply|push|all]
#
# Prerequisites:
#   • AWS CLI configured (aws configure or IAM role/instance profile)
#   • Docker running
#   • Terraform >= 1.5 installed

set -euo pipefail

COMMAND="${1:-all}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"

log() { echo "[$(date +%T)] $*"; }

# ── terraform init ─────────────────────────────────────────────────────────

tf_init() {
  log "Running terraform init..."
  cd "$SCRIPT_DIR"
  terraform init -upgrade
}

# ── terraform plan ─────────────────────────────────────────────────────────

tf_plan() {
  log "Running terraform plan..."
  cd "$SCRIPT_DIR"
  terraform plan -out=tfplan
}

# ── terraform apply ────────────────────────────────────────────────────────

tf_apply() {
  log "Running terraform apply..."
  cd "$SCRIPT_DIR"
  if [[ -f tfplan ]]; then
    terraform apply tfplan
  else
    terraform apply -auto-approve
  fi
}

# ── docker build + push ────────────────────────────────────────────────────

docker_push() {
  cd "$SCRIPT_DIR"

  ECR_URL=$(terraform output -raw ecr_repository_url)
  AWS_REGION=$(terraform output -raw vpc_id | xargs -I{} aws ec2 describe-vpcs --vpc-ids {} --query 'Vpcs[0].Tags[?Key==`Name`].Value' --output text 2>/dev/null || echo "us-east-1")
  # Simpler: grab region from tfvars or AWS config
  AWS_REGION=$(aws configure get region || echo "us-east-1")

  log "Authenticating Docker to ECR..."
  aws ecr get-login-password --region "$AWS_REGION" \
    | docker login --username AWS --password-stdin "$ECR_URL"

  log "Building Docker image..."
  docker build --platform linux/amd64 -t "${ECR_URL}:latest" "$APP_DIR"

  log "Pushing image to ECR..."
  docker push "${ECR_URL}:latest"

  log "Image pushed: ${ECR_URL}:latest"
}

# ── force ECS redeployment ─────────────────────────────────────────────────

ecs_redeploy() {
  cd "$SCRIPT_DIR"

  CLUSTER=$(terraform output -raw ecs_cluster_name)
  SERVICE=$(terraform output -raw ecs_service_name)
  AWS_REGION=$(aws configure get region || echo "us-east-1")

  log "Forcing new ECS deployment..."
  aws ecs update-service \
    --cluster "$CLUSTER" \
    --service "$SERVICE" \
    --force-new-deployment \
    --region "$AWS_REGION" \
    --query 'service.deployments[0].status' \
    --output text

  log "Deployment triggered. Monitor with:"
  log "  aws ecs describe-services --cluster $CLUSTER --services $SERVICE --region $AWS_REGION"
  log ""
  log "App URL: $(terraform output -raw app_url)"
}

# ── main ──────────────────────────────────────────────────────────────────

case "$COMMAND" in
  init)   tf_init ;;
  plan)   tf_init && tf_plan ;;
  apply)  tf_init && tf_apply ;;
  push)   docker_push && ecs_redeploy ;;
  all)
    tf_init
    tf_apply
    docker_push
    ecs_redeploy
    ;;
  *)
    echo "Usage: $0 [init|plan|apply|push|all]"
    exit 1
    ;;
esac
