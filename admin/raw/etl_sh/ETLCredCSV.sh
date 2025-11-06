#!/bin/bash
sed -n '1p' "$1" > ./output/temp.csv

truncate -s -1  "./output/temp.csv"
truncate -s -1  "./output/temp.csv"

# ...existing code...
sed -i -E 's/^([^.]+),([^.]+),([^.]+)/"\1\";"\2\";"\3\"/' "./output/temp.csv"

file1=$1
FILE_CSV="./output/$(basename "${file1%.csv}_temp.csv")"

# acrescenta ao header as colunas extras com aspas e separador ;
sed -e '1s/$/;\"nome_arquivo\";\"data_base\"/' "./output/temp.csv" > "$FILE_CSV"
echo >> "$FILE_CSV"

# # # for file in "$CSV_SAIDA"; do
echo "Convertendo $FILE_CSV..."

# Extrai a data base do nome do arquivo, exemplo: custo_2025_01.xls -> 2025_01 transforma em dd/mm/yyyy
data_base=$(echo $1 | grep -oP '\d{4}_\d{2}' | awk -F'_' '{printf "01/%s/%s", $2, $1}')

# Gera CSV temporário
grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2},[^.]+' $1 > "./output/temp.csv"

# ...existing code...
# Adiciona colunas extras (nome_arquivo, data_base) ao CSV, pulando o header se não for o primeiro arquivo
awk -v fname="$(basename $1)" -v dbase="$data_base" 'BEGIN{ORS="\r\n"} {sub(/\r$/, "", $0)} FNR>1 {print $0 "," fname "," dbase }' "./output/temp.csv" > "./output/temp2.csv"

awk -F',' 'BEGIN{OFS=","} NR==0{print; next} { if($1 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) { split($1,a,"-"); $1=a[3]"/"a[2]"/"a[1] } print }' "./output/temp2.csv" > "$FILE_CSV"

sed -i -E 's/^([0-9]{2}\/[0-9]{2}\/[0-9]{4}),([^\w]+\w|[^\W]+\w|[^,]*),(\-[0-9.]+|[0-9.]+),([^\w]+\w),([0-9]{2}\/[0-9]{2}\/[0-9]{4})/\1,\2,,\3,\4,\5/' "$FILE_CSV"

# sed -i -E 's/^([0-9]{2}\/[0-9]{2}\/[0-9]{4}),([^\w]+\w),([^\w]+\w|),(\-[0-9.]+|[0-9.]+),([^\w]+\w),([0-9]{2}\/[0-9]{2}\/[0-9]{4})/"\1\";"\2\";"\3\";"\4\";"\5\";"\6\"/' "$FILE_CSV"
