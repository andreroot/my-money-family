variable "GIT_REPOSITORY_NAME" {
    default = "pipeline-site-receita-s3"
    type = string
}

variable "region" {
  default = "us-east-1"
  type    = string
}


variable "aws_access_key_id" {
  type        = string
}

variable "vpc_id" {
  default = "my-ec2-vpc"
  type        = string
}

variable "subnet_id" {
  default = "my-ec2-subnet"
  type        = string
}

variable "aws_secret_access_key" {
  type        = string  
}

variable "environment" {
  default = "staging"
  type        = string  
}

# variable "instance_name" {
#   description = "Tag Name da instância"
#   type        = string
#   default     = "my-ec2"
# }

# variable "instance_user" {
#   description = "Usuário padrão da AMI (ec2-user ou ubuntu)"
#   type        = string
#   default     = "ec2-user"
# }

# variable "ECR_REPOSITORY_REGISTRY" {
#   default = "967201331463.dkr.ecr.us-east-1.amazonaws.com"
#   type        = string
# }

# variable "ECR_REPOSITORY_NAME" {
#   default = "dados_webhook_integ"
#   type        = string
# }