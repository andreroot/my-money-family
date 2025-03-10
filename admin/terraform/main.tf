terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
      
    }
  }

}

provider "google" {
  #credentials = file("<path_to_your_service_account_key>.json")
  project     = "297545123791"
  region      = "us-central1"
}

resource "google_storage_bucket" "example_bucket" {
  name     = "example-bucket-name-9999"
  location = "US"
  
}


# terraform init -backend-config=".\enviroments\staging\main.tf"
# terraform workspace new staging
# terraform plain
# terraform apply
# terraform destroy