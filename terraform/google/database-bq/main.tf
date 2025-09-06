terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
      
    }
  }

}

provider "google" {
  credentials = file("/home/andre/.ssh/my-chave-gcp-devsamelo2.json")
  project     = "devsamelo2"
  region      = "us-east1"
}


# terraform init -backend-config=".\enviroments\staging\main.tf"
# terraform workspace new staging
# terraform plain
# terraform apply
# terraform destroy