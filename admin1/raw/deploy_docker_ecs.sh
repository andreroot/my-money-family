# aws ecr get-login-password --region us-east-1 \
#   | docker login --username AWS --password-stdin <AWS_ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com

# docker build -t meu-app .

# docker tag meu-app:latest <AWS_ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/meu-app:latest

# docker push <AWS_ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com/meu-app:latest

#!/bin/bash
set -e

echo "AWS | PROCESSO MEDALION VIA DOCKER - INÍCIO  🐋"

# ==== CONFIGURAÇÕES ====
AWS_REGION="us-east-1"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REPO_NAME="raw-my-cust"
IMAGE_TAG="latest"

# ==== LOGIN NO ECR ====
echo "[1/4] Fazendo login no ECR..."
aws ecr get-login-password --region $AWS_REGION \
  | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# ==== BUILD DA IMAGEM ====
echo "[2/4] Construindo imagem Docker..."
# docker build -t $REPO_NAME .
cd /home/andre/projetos/my-money-family/admin/raw
docker compose build

# ==== TAG ====
echo "[3/4] Criando tag para o ECR..."
# docker tag $REPO_NAME:$IMAGE_TAG $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:$IMAGE_TAG
docker tag raw-my-cust:$IMAGE_TAG $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/raw-my-cust:$IMAGE_TAG

# ==== CREATE REPOSITORY ====
# aws ecr create-repository --repository-name $REPO_NAME --region $AWS_REGION

# ==== PUSH /ATUALIZACAIO DE DOCKER IMAGE ====
echo "[4/4] Enviando imagem para o ECR..."
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:$IMAGE_TAG

echo "✅ Imagem enviada com sucesso para:"
echo "   $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO_NAME:$IMAGE_TAG"


# ==== ATUALIZA FONTES NO S3 s3://medalion-cust/raw/original ====
echo "Atualizando arquivos no S3..."
aws s3 cp "/mnt/c/Users/andre/Documents/github/my-money-family/admin/etl/excel" "s3://medalion-cust/raw/original/" --recursive --exclude "*" --include "credito_2025_*.xls"
aws s3 cp "/mnt/c/Users/andre/Documents/github/my-money-family/admin/etl/excel" "s3://medalion-cust/raw/original/" --recursive --exclude "*" --include "custo_2025_*.xls"

# ==== EXECUTE PIPELINE ====
echo "Executando..."
curl -X POST "https://4ebtfw1bec.execute-api.us-east-1.amazonaws.com/run"