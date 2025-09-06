terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
      
    }
  }

}

# provider "google" {
#   #credentials = file("<path_to_your_service_account_key>.json")
#   project     = "297545123791"
#   region      = "us-central1"
# }


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