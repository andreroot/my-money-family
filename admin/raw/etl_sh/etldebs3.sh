#!/bin/bash
# Script para unir arquivos XLS a partir da linha 9 em um único CSV

# Caminho do bucket S3 e prefixo dos arquivos
S3_BUCKET="s3://medalion-cust/raw"
S3_PATTERN="custo_2025_01" #s3://medalion-cust/raw/original/custo_2025_01.xls

# Nome do CSV de saída
CSV_SAIDA="./output/extrato_2025.csv"
TMP="./output/temp.csv"

# Limpa o arquivo de saída se já existir
> "$CSV_SAIDA"

# Baixa arquivos do S3 para o diretório local
aws s3 cp "$S3_BUCKET/original/" "./output/" --recursive --exclude "*" --include "custo_2025_09.xls"
sleep 5

# Cabeçalho do primeiro arquivo
for file in /app/output/custo_2025_01.xls; do
  echo "Pegar os nomes de colunas do primeiro arquivo $file..."
  in2csv --sheet "Lançamentos" --skip-lines 8 "$file" | head -n 1 > "$TMP"
  awk 'NR==1 {print $0",nome_arquivo,data_base"} NR>1' "$TMP" >> "$CSV_SAIDA"
  rm "$TMP"
done

sleep 5


# Loop pelos arquivos baixados do S3
for file in ./output/custo_2025_*.xls; do

    # Verifica se o arquivo existe
    if [[ -f "$file" ]]; then
        # # Se for ODS, converte para XLSX
        # if [[ "$file" == *.ods ]]; then
        #     echo "Convertendo $file de ODS para XLSX..."
        #     xlsx_file="${file%.ods}.xls"
        #     libreoffice --headless --convert-to xls "$file" --outdir "$(dirname "$file")"
        #     file="$xlsx_file"
        # fi
      in2csv --sheet "Lançamentos" --skip-lines 8 "$file" 1>/dev/null 2>./output/error.log
      if grep -q "Openoffice.org ODS file; not supported" ./output/error.log; then
          echo "Arquivo $file é ODS (mesmo com extensão .xls), convertendo para XLS..."
          cp "$file" "${file%.xls}.ods"
          # rm -f "$file"
          xlsx_file="${file%.xls}.xls"
          libreoffice --headless --convert-to xls "${file%.xls}.ods" --outdir "$(dirname "$file")"
          rm -f "${file%.xls}.ods"
          file="$xlsx_file"
      fi
      rm -f ./output/error.log

      echo "Convertendo $file..."
      data_base=$(echo "$file" | grep -oP '\d{4}_\d{2}' | awk -F'_' '{printf "01/%s/%s\n", $2, $1}')
      in2csv --sheet "Lançamentos" --skip-lines 8 "$file" | tail -n +1 > "$TMP"
      awk -v fname="$(basename "$file")" -v dbase="$data_base" 'FNR>1 {print $0","fname","dbase}' "$TMP" >> "$CSV_SAIDA"
      rm "$TMP"

    fi
done

aws s3 cp "$CSV_SAIDA" "$S3_BUCKET/output/extrato_2025.csv"

echo "Conversão finalizada. Arquivo gerado: $S3_BUCKET/output/extrato_2025.csv"