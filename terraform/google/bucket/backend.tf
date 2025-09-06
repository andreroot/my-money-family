terraform {
  backend "gcs" {
    bucket = "projeto-terraform"
    prefix = "backend-staging-buckt"
  }
}

## criar os buckets para backend
## liberar acesso para administrador bucket