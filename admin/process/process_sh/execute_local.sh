#!/bin/bash
set -e

ANO=$1
echo "Ano recebido: $ANO"

# TYPE_DOC=$2
# echo "Tipo de documento: $TYPE_DOC"

# Instalação completa do csvkit + dependências para Excel
echo "🤖 Processo Medallion - PROCESS - Início da instalação 🐋"

echo "🐼 Execução python do ETL PROCESS"
python3 ./src/main.py $ANO
