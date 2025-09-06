variable "environment" {
  default="staging"
}

variable "project" { 
  default ="devsamelo2"
}

variable "credentials_file" { 
    default ="/home/andre/.ssh/my-chave-gcp-devsamelo2.json"

}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-c"
}

variable "dataset_id" {
  description = "The name of the dataset."
  type        = string
  default     = "dev_domestico"
}