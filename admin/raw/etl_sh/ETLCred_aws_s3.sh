#!/bin/bash
# Script para unir arquivos XLS a partir da linha 9 em um único CSV
ANO=$1
echo "Ano recebido: $ANO"

# Caminho do bucket S3 e prefixo dos arquivos
S3_BUCKET="s3://medalion-cust/raw"
# fontes originais no s3
#s3://medalion-cust/raw/original/

# Nome do CSV de saída
CSV_SAIDA="./output/credito_${ANO}.csv"
TMP="./output/temp.csv"

# Limpa o arquivo de saída se já existir
> "$CSV_SAIDA"

# # Baixa arquivos do S3 para o diretório local
aws s3 cp "$S3_BUCKET/original/" "./output/" --recursive --exclude "*" --include "credito_${ANO}_*"

sleep 5

# Loop pelos arquivos que contêm "custo" no nome
for file in ./output/credito_${ANO}_01*; do
  echo "Pegar os nomes de colunas do primeiro arquivo $file..."
  # Pega o cabeçalho só do primeiro arquivo
  #in2csv --sheet "Lançamentos" --skip-lines 1 "$file" | head -n 1 > "$TMP"
  in2csv --sheet "Lançamentos" --skip-lines 1 "$file" 2>/dev/null | head -n 1 | awk -F',' 'BEGIN{OFS=","} {for(i=1;i<=NF;i++) if($i=="c") $i="coluna_sem_nome_"i; OFS=",";print $0}' > "$TMP"
  awk 'BEGIN{OFS=","} NR==1 {print $0",nome_arquivo,data_base"} NR>1' "$TMP" >> "$CSV_SAIDA"
  sed -i -E 's/^([^.]+),([^.]+),([^.]+),([^.]+),([^.]+),([^.]+)/"\1\";"\2\";"\3\";"\4\";"\5\";"\6\"/' "$CSV_SAIDA"
  rm "$TMP"
done


# Loop pelos arquivos que contêm "custo" no nome
for file in ./output/credito_${ANO}_*.xls; do

  echo "Convertendo $file..."

  # Verifica se o arquivo existe
  if [[ -f "$file" ]]; then
    # # Se for ODS, converte para XLSX
    # if [[ "$file" == *.ods ]]; then
    #     echo "Convertendo $file de ODS para XLSX..."
    #     xlsx_file="${file%.ods}.xls"
    #     libreoffice --headless --convert-to xls "$file" --outdir "$(dirname "$file")"
    #     file="$xlsx_file"
    # fi
    in2csv --sheet "Lançamentos" --skip-lines 1 "$file" 1>/dev/null 2>./output/error.log

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

    # in2csv --sheet "Lançamentos" --skip-lines 8 "$file" | tail -n +2 >> "$CSV_SAIDA"
    # Extrai a data base do nome do arquivo, exemplo: custo_2025_01.xls -> 2025_01
    data_base=$(echo "$file" | grep -oP '\d{4}_\d{2}' | awk -F'_' '{printf "01/%s/%s\n", $2, $1}')

    # Gera CSV temporário
    in2csv --sheet "Lançamentos" --skip-lines 1 "$file" 2>/dev/null | tail -n +1 > "$TMP"

    # Adiciona colunas extras (nome_arquivo, data_base) ao CSV, pulando o header se não for o primeiro arquivo
    awk -v fname="$(basename "$file")" -v dbase="$data_base" 'FNR>1 {print $0","fname","dbase}' "$TMP" >> "$CSV_SAIDA"
    #awk -F',' 'NR==1 {for(i=1;i<=NF;i++) if($i=="") $i="coluna_sem_nome_"i; print $0} NR>1' "$TMP" > "$CSV_SAIDA"
    
    # sed -i -E 's/^([0-9]{2}\/[0-9]{2}\/[0-9]{4}),([^\w]+\w),([^\w]+\w|),(\-[0-9.]+|[0-9.]+),([^\w]+\w),([0-9]{2}\/[0-9]{2}\/[0-9]{4})/"\1\";"\2\";"\3\";"\4\";"\5\";"\6\"/' "$CSV_SAIDA"

    sed -i -E 's/^([0-9]{2}\/[0-9]{2}\/[0-9]{4}),([^\w]+\w|[^\W]+\w|[^,]*),([^\w]+\w|),(\-[0-9.]+|[0-9.]+),([^\w]+\w),([0-9]{2}\/[0-9]{2}\/[0-9]{4})/\1,\2,\3,\4,\5,\6/' "$CSV_SAIDA"

    rm "$TMP"
  fi
done


# Loop pelos arquivos que contêm "credito" no formato csv no nome
for filecsv in ./output/credito_2025_*.csv; do
    
    sleep 5

    echo "Convertendo arquivo CSV $filecsv..."

    # ./etl_sh/ETLCredCSV.sh $filecsv
    # REGRA DE TRANSFORMAÇAÕ DE CSV PARA FORMATO
    sed -n '1p' "$filecsv" > ./output/temp.csv

    truncate -s -1  "./output/temp.csv"
    truncate -s -1  "./output/temp.csv"

    # ...existing code...
    sed -i -E 's/^([^.]+),([^.]+),([^.]+)/"\1\";"\2\";"\3\"/' "./output/temp.csv"

    file1=$filecsv
    FILE_CSV="./output/$(basename "${file1%.csv}_temp.csv")"

    # acrescenta ao header as colunas extras com aspas e separador ;
    sed -e '1s/$/;\"nome_arquivo\";\"data_base\"/' "./output/temp.csv" > "$FILE_CSV"
    echo >> "$FILE_CSV"

    # # # for file in "$CSV_SAIDA"; do
    echo "Convertendo $FILE_CSV..."

    # Extrai a data base do nome do arquivo, exemplo: custo_2025_01.xls -> 2025_01 transforma em dd/mm/yyyy
    data_base=$(echo $filecsv | grep -oP '\d{4}_\d{2}' | awk -F'_' '{printf "01/%s/%s", $2, $1}')

    # Gera CSV temporário
    grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2},[^.]+' $filecsv > "./output/temp.csv"

    echo "etapa 1 - conversao de datas: ./output/temp.csv"
    grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2},[^.]+' $filecsv
    awk -v fname="$(basename $filecsv)" -v dbase="$data_base" 'BEGIN{ORS="\r\n"} {sub(/\r$/, "", $0)} FNR>1 {print $0 "," fname "," dbase }' "./output/temp.csv"
    cat ./output/temp.csv

    # ...existing code...
    # Adiciona colunas extras (nome_arquivo, data_base) ao CSV, pulando o header se não for o primeiro arquivo
    awk -v fname="$(basename $filecsv)" -v dbase="$data_base" 'BEGIN{ORS="\r\n"} {sub(/\r$/, "", $0)} FNR>1 {print $0 "," fname "," dbase }' "./output/temp.csv" > "./output/temp2.csv"

    echo "etapa 1 - conversao de datas: ./output/temp2.csv"
    cat ./output/temp2.csv

    echo "etapa 2 - conversao de datas: ./output/temp2.csv"
    awk -F',' 'BEGIN{OFS=","} 
    {
      # Se o campo 1 está no formato yyyy-mm-dd, troca para dd/mm/yyyy
      if ($1 ~ /^[0-9]$/) {
        split($1, a, "-");
        $1 = a[3] "/" a[2] "/" a[1];
      }
      print $1
    }' "./output/temp2.csv"
    
    # 
    awk -F',' 'BEGIN{OFS=","} { if($1 ~ /^[0-9]/) { split($1,a,"-"); $1=a[3]"/"a[2]"/"a[1] } print }' "./output/temp2.csv" > "./output/temp3.csv"
    
    echo "etapa 3 - conversao de datas: ./output/temp3.csv"
    cat ./output/temp3.csv

    sed -i -E 's/^([0-9]{2}\/[0-9]{2}\/[0-9]{4}),([^\w]+\w|[^\W]+\w|[^,]*),(\-[0-9.]+|[0-9.]+),([^\w]+\w),([0-9]{2}\/[0-9]{2}\/[0-9]{4})/\1,\2,,\3,\4,\5/' "./output/temp3.csv"

    # sed -i -E 's/^([0-9]{2}\/[0-9]{2}\/[0-9]{4}),([^\w]+\w),([^\w]+\w|),(\-[0-9.]+|[0-9.]+),([^\w]+\w),([0-9]{2}\/[0-9]{2}\/[0-9]{4})/"\1\";"\2\";"\3\";"\4\";"\5\";"\6\"/' "$FILE_CSV"

    echo "Copiar ./output/temp3.csv para arquivo final..."

    cat ./output/temp3.csv

    awk 'BEGIN{ORS="\r\n"} {sub(/\r$/, "", $0)} FNR>1 {print $0 }' "./output/temp3.csv" >> "$CSV_SAIDA"

    # sleep 5

    # file2="$(basename "${filecsv%.csv}_temp.csv")"
    
    # if [[ -f "./output/$file2" ]]; then
    #     # Gera CSV temporário
    #     echo "Copiar ./output/$file2 para arquivo final..."

    #     # in2csv --sheet "Lançamentos" --skip-lines 1 "$file" 2>/dev/null | tail -n +1

    #     awk 'BEGIN{ORS="\r\n"} {sub(/\r$/, "", $0)} FNR>1 {print $0 }' "./output/temp.csv" 2>/dev/null >> "$CSV_SAIDA"
    #     # | awk -F';' 'BEGIN{OFS=";"} NR==1{print; next} { if($1 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) { split($1,a,"-"); $1=a[3]"/"a[2]"/"a[1] } print }' \
    #     # | awk -F';' 'BEGIN{OFS=";"} NR==1{print; next} { if($5 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) { split($5,a,"-"); $5=a[3]"/"a[2]"/"a[1] } print }' 

    #     rm -rf "./output/$file2"
    # fi
    
done

echo "Conversão finalizada. Arquivo gerado: $CSV_SAIDA"

S3_BUCKET_BRONZE="s3://medalion-cust/bronze"

sed -i -E 's/^([0-9]{2}\/[0-9]{2}\/[0-9]{4}),([^\w]+\w|[^\W]+\w|[^,]*),([^\w]+\w|[^\W]+\w|),(\-[0-9.]+|[0-9.]+),([^\w]+\w),([0-9]{2}\/[0-9]{2}\/[0-9]{4})/"\1\";"\2\";"\3\";"\4\";"\5\";"\6\"/' "$CSV_SAIDA"

aws s3 cp "$CSV_SAIDA" "$S3_BUCKET_BRONZE/credito_${ANO}.csv"

echo "Conversão finalizada. Arquivo gerado: $S3_BUCKET_BRONZE/credito_${ANO}.csv"


