#!/bin/bash
# Script para unir arquivos XLS a partir da linha 9 em um único CSV
ANO=$1
TIPO_CRED=$2
echo "Ano recebido: $ANO"

# Caminho do bucket S3 e prefixo dos arquivos
S3_BUCKET="s3://medalion-cust/raw"
# fontes originais no s3
#s3://medalion-cust/raw/original/

# Nome do CSV de saída
CSV_SAIDA="./output/credito_${TIPO_CRED}_${ANO}.csv"
TMP="./output/temp.csv"
TMP2="./output/temp2.csv"

TMPH="./output/temph.csv"
TMPH2="./output/temph2.csv"

# Limpa o arquivo de saída se já existir
> "$CSV_SAIDA"

# # Baixa arquivos do S3 para o diretório local
aws s3 cp "$S3_BUCKET/original/" "./output/" --recursive --exclude "*" --include "credito_${TIPO_CRED}_${ANO}_*"


# Loop pelos arquivos que contêm "custo" no nome
for file in ./output/credito_${TIPO_CRED}_${ANO}*.csv; do

  # Verifica se o arquivo existe
  if [[ $(basename "$file") == *"_01.csv" ]]; then
    echo "Pegar os nomes de colunas do primeiro arquivo $file..."
    # Pega o cabeçalho só do primeiro arquivo | duplicar  linhas do arquivo para gerar cabeçalho correto
    awk '{print; print}'  "$file" > "$TMPH"
    # in2csv --skip-lines 1 "$TMPH" | head -n 1 > "$TMPH2"
    # in2csv --snifflimit 0 --delimiter="," "$TMPH" | head -n 1 > "$TMPH2"
    head -n 1 "$TMPH" > "$TMPH2"
    truncate -s -1  "$TMPH2"
    # truncate -s -1  "./output/temp.csv"
    sed -i '/^$/d' "$TMPH2"
    awk 'NR==1 {print $0} NR>1' "$TMPH2" >> "$CSV_SAIDA" #",nome_arquivo,data_base"
    #sed -i -E 's/([^.]+),([^.]+),([^.]+)/"\1";"\2";"\3";"nome_arquivo";"data_base"/' "$CSV_SAIDA"
    sed -i -E 's/(data)\,(lançamento)\,(valor)/"\1";"\2";"";"\3";"nome_arquivo";"data_base"/'  "$CSV_SAIDA"
  fi
done

# Loop pelos arquivos que contêm "credito" no formato csv no nome
for filecsv in ./output/credito_${TIPO_CRED}_${ANO}_*.csv; do
    
    sleep 5

    echo "Convertendo arquivo CSV $filecsv..."

    ./etl_sh/ETLCredCSV.sh $filecsv

   # ADD LINHAS NOVAS NO ARQUIVO FINAL
    awk 'BEGIN{ORS="\r\n"} {sub(/\r$/, "", $0)} FNR>0 {print $0 }' "./output/temp3.csv" >> "$CSV_SAIDA"

    FILE_CSV="./output/$(basename "${filecsv%.csv}_temp.csv")"
    rm "$FILE_CSV"
done


# REMOVER ARQUIVOS TEMPORÁRIOS
rm "./output/temp.csv" "./output/temp2.csv" "./output/temp3.csv"
rm "./output/temph.csv" "./output/temph2.csv"
    
echo "Conversão finalizada. Arquivo gerado: $CSV_SAIDA"

S3_BUCKET_BRONZE="s3://medalion-cust/bronze"

sed -i -E 's/^([0-9]{2}\/[0-9]{2}\/[0-9]{4}),([^\w]+\w|[^\W]+\w|[^,]*),([^\w]+\w|[^\W]+\w|),(\-[0-9.]+|[0-9.]+),([^\w]+\w),([0-9]{2}\/[0-9]{2}\/[0-9]{4})/"\1\";"\2\";"\3\";"\4\";"\5\";"\6\"/' "$CSV_SAIDA"

aws s3 cp "$CSV_SAIDA" "$S3_BUCKET_BRONZE/credito_${TIPO_CRED}_${ANO}.csv"

echo "Conversão finalizada. Arquivo gerado: $S3_BUCKET_BRONZE/credito_${TIPO_CRED}_${ANO}.csv"


