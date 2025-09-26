#!/bin/bash
# Script para unir arquivos XLS a partir da linha 9 em um único CSV

# Caminho do bucket S3 e prefixo dos arquivos
S3_BUCKET="s3://medalion-cust/raw"
# fontes originais no s3
#s3://medalion-cust/raw/original/

# Nome do CSV de saída
CSV_SAIDA="/app/output/credito_2025.csv"
TMP="/app/output/temp.csv"

# Limpa o arquivo de saída se já existir
> "$CSV_SAIDA"

# Baixa arquivos do S3 para o diretório local
aws s3 cp "$S3_BUCKET/original/" "/app/output/" --recursive --exclude "*" --include "credito_2025_*.xls"

sleep 5

# Loop pelos arquivos que contêm "custo" no nome
for file in /app/output/credito_2025_01*; do
  echo "Pegar os nomes de colunas do primeiro arquivo $file..."
  # Pega o cabeçalho só do primeiro arquivo
  #in2csv --sheet "Lançamentos" --skip-lines 1 "$file" | head -n 1 > "$TMP"
  in2csv --sheet "Lançamentos" --skip-lines 1 "$file" 2>/dev/null | head -n 1 | awk -F',' 'BEGIN{OFS=","} {for(i=1;i<=NF;i++) if($i=="c") $i="coluna_sem_nome_"i; OFS=",";print $0}' > "$TMP"
  awk 'BEGIN{OFS=","} NR==1 {print $0",nome_arquivo,data_base"} NR>1' "$TMP" >> "$CSV_SAIDA"
  rm "$TMP"
done


# Loop pelos arquivos que contêm "custo" no nome
for file in /app/output/credito_2025*; do
    echo "Convertendo $file..."
    # in2csv --sheet "Lançamentos" --skip-lines 8 "$file" | tail -n +2 >> "$CSV_SAIDA"
    # Extrai a data base do nome do arquivo, exemplo: custo_2025_01.xls -> 2025_01
    data_base=$(echo "$file" | grep -oP '\d{4}_\d{2}' | awk -F'_' '{printf "01/%s/%s\n", $2, $1}')
    # Gera CSV temporário
    in2csv --sheet "Lançamentos" --skip-lines 1 "$file" 2>/dev/null | tail -n +1 > "$TMP"
    # Adiciona colunas extras (nome_arquivo, data_base) ao CSV, pulando o header se não for o primeiro arquivo
    awk -v fname="$(basename "$file")" -v dbase="$data_base" 'FNR>1 {print $0","fname","dbase}' "$TMP" >> "$CSV_SAIDA"
    #awk -F',' 'NR==1 {for(i=1;i<=NF;i++) if($i=="") $i="coluna_sem_nome_"i; print $0} NR>1' "$TMP" > "$CSV_SAIDA"
    rm "$TMP"
done

echo "Conversão finalizada. Arquivo gerado: $CSV_SAIDA"

aws s3 cp "$CSV_SAIDA" "$S3_BUCKET/output/credito_2025.csv"

echo "Conversão finalizada. Arquivo gerado: $S3_BUCKET/output/credito_2025.csv"