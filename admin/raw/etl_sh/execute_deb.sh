#!/bin/bash
set -e

ANO=$1
echo "Ano recebido: $ANO"

# Instalação completa do csvkit + dependências para Excel
echo "🤖 Processo Medallion - RAW - Início da instalação 🐋"

echo "🐧 Sheel Script - RAW Debito"
## EXECUTE DEBITO S3
./etl_sh/ETLDeb_aws_s3.sh $ANO

echo "🐼 Execução python do ETL RAW"
python3 /app/src/main.py $ANO 'debito'
