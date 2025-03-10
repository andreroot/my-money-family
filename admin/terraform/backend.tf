terraform {
  backend "gcs" {
    bucket = "projeto-terraform"
    prefix = "backend-staging"
  }
}

## criar os buckets para backend
## liberar acesso para administrador bucket