output "app_url" {
  description = "Public URL of the FIPSAR Streamlit application (HTTP)"
  value       = "http://${module.alb.dns_name}"
}

output "ecr_repository_url" {
  description = "ECR repository URL — use this in your docker push command"
  value       = module.ecr.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = module.ecs_cluster.cluster_name
}

output "ecs_service_name" {
  description = "ECS service name"
  value       = aws_ecs_service.app.name
}

output "cloudwatch_log_group" {
  description = "CloudWatch log group for container logs"
  value       = aws_cloudwatch_log_group.app.name
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "deploy_commands" {
  description = "Commands to build and push the Docker image after first apply"
  value       = <<-EOT
    # 1. Authenticate Docker to ECR
    aws ecr get-login-password --region ${var.aws_region} | \
      docker login --username AWS --password-stdin ${module.ecr.repository_url}

    # 2. Build and push
    docker build -t ${module.ecr.repository_url}:latest ..
    docker push ${module.ecr.repository_url}:latest

    # 3. Force ECS to pull the new image
    aws ecs update-service \
      --cluster ${module.ecs_cluster.cluster_name} \
      --service ${aws_ecs_service.app.name} \
      --force-new-deployment \
      --region ${var.aws_region}
  EOT
}
