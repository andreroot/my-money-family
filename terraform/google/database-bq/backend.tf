terraform {
  backend "gcs" {
    bucket = "projeto-terraform"
    prefix = "backend-staging-bq"
  }
}

## criar os buckets para backend
## liberar acesso para administrador bucket