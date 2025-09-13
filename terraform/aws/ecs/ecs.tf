
# 1. ECR Repository
# resource "aws_ecr_repository" "app_repo" {
#   name = "raw-my-cust"
# }

# 2. ECS Cluster
resource "aws_ecs_cluster" "app_cluster" {
  name = "app-cluster"
}

# 3. ECS Task Definition
resource "aws_ecs_task_definition" "app_task" {
  family                   = "app-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn

  volume {
    name = "output_volume"
  }
  
  container_definitions = <<DEFINITION
  [
    {
      "name": "meu-container",
      "image": "707562827963.dkr.ecr.us-east-1.amazonaws.com/raw-my-cust:latest",
      "mountPoints": [
        {
          "sourceVolume": "output_volume",
          "containerPath": "/tmp/output"
        }
      ],
      "essential": true,
      "memory": 512,
      "cpu": 256,
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/meu-app",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
  DEFINITION
}

# 4. Log Group
resource "aws_cloudwatch_log_group" "app_logs" {
  name              = "/ecs/meu-app"
  retention_in_days = 0
}


