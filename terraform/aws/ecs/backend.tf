terraform {
#criar dentro do workspace definido no init
#{workspace_key_prefix}/{workspace_name}/{key} 
  backend "s3" {
    bucket         = "my-andre-terraform"
    key            = "ecs/terraform.tfstate"
    region         = "us-east-1"
    workspace_key_prefix = "terraform_mycust"
   }
}