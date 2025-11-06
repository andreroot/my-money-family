#!/bin/bash
# Script para unir arquivos XLS a partir da linha 9 em um único CSV
ANO=$1
echo "Ano recebido: $ANO"

# # Caminho do bucket S3 e prefixo dos arquivos
# S3_BUCKET="s3://medalion-cust/raw"
# # fontes originais no s3
# #s3://medalion-cust/raw/original/

# Nome do CSV de saída
CSV_SAIDA="./output/credito_${ANO}.csv"
TMP="./output/temp.csv"

> "$TMP"
# Limpa o arquivo de saída se já existir
> "$CSV_SAIDA"

# # Baixa arquivos do S3 para o diretório local
# aws s3 cp "$S3_BUCKET/original/" "./output/" --recursive --exclude "*" --include "credito_${ANO}_*.csv"
cp /mnt/c/Users/andre/Documents/github/my-money-family/admin/etl/excel/credito_2025_*.csv ./output/

# csvstack $PATH_LOCAL_RAW/*ESTABELE -e="latin-1" -v --tabs > $PATH_LOCAL_PROCESS

# sed -i -e 's/\;\""/"\;\"/;s/""\;\""/"\;\"/g;s/;\""""/;\"\"/' $PATH_LOCAL_PROCESS

# grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2},' "./output/credito_2025_211.csv" | sed -E 's/^([0-9]{4}-[0-9]{2}-[0-9]{2})/g;s/"1' 
# grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2},[^,]+' "./output/credito_2025_211.csv" | sed -E 's/^([0-9]{4}-[0-9]{2}-[0-9]{2}),([^,]+),([0-9.]+)/"\1\";"\2\";"\3\"/' > ./output/temp2.csv
# grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2},[^,]+' "./output/credito_2025_211.csv" | awk -F',' 'BEGIN {OFS=","}{print $3}'
# {for(i=1;i<=NF;i++) if(match($i, /, *[0-9]+(\.[0-9]+)?$/)) $i=substr($0, 1, RSTART-1) ; OFS=",";print $0}

# com for 
# grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2},[^,]+' "./output/credito_2025_211.csv" | \
# awk -F',' 'BEGIN{OFS=","} {for(i=1;i<=NF;i++) if(match($3, /[0-9].[0-9]?$/)) $3=$3"" ; OFS=",";print $3}'

# sem for


# sed -E 's/^([0-9]{4}-[0-9]{2}-[0-9]{2}),([^,]+),([^,]+|)/"\1\"/' ./output/temp2.csv

# sleep 5

# Loop pelos arquivos que contêm "custo" no nome
for file in ./output/credito_2025_11; do
  rm "$TMP"
  echo "Pegar os nomes de colunas do primeiro arquivo $file..."
  # Pega o cabeçalho só do primeiro arquivo
  #in2csv --sheet "Lançamentos" --skip-lines 1 "$file" | head -n 1 > "$TMP" 2>/dev/null
  header=$(head -n 1 "$file.csv")
  echo $header
  sed -n '1p' ./output/credito_2025_11.csv > "$TMP"

done

# sleep 5
# # tr -d '\n' < "$TMP" > "./output/temp2.csv"
# truncate -s -1  "./output/temp.csv"
# # head -n 1 "$file.csv" | awk -F',' 'BEGIN{OFS=","} {for(i=1;i<=NF;i++) if($i=="c") $i="coluna_sem_nome_"i; OFS=",";print $0}' > "$TMP"
# # awk 'BEGIN{OFS=","} NR==1 {print $0",nome_arquivo,data_base"} NR>1' "$TMP" >> "$CSV_SAIDA"
# # # rm "$TMP"
# # 
# # # # sed -i '$d' "$CSV_SAIDA"
truncate -s -1  "./output/temp.csv"
sed -i '1s/$/,nome_arquivo,data_base/' "./output/temp.csv" >> "$CSV_SAIDA"
rm "$TMP"

# # Loop pelos arquivos que contêm "custo" no nome
# for file in ./output/credito_${ANO}_01*; do
#   echo "Pegar os nomes de colunas do primeiro arquivo $file..."
#   # Pega o cabeçalho só do primeiro arquivo
#   #in2csv --sheet "Lançamentos" --skip-lines 1 "$file" | head -n 1 > "$TMP"
#   in2csv --sheet "Lançamentos" --skip-lines 1 "$file" 2>/dev/null | head -n 1 | awk -F',' 'BEGIN{OFS=","} {for(i=1;i<=NF;i++) if($i=="c") $i="coluna_sem_nome_"i; OFS=",";print $0}' > "$TMP"
#   awk 'BEGIN{OFS=","} NR==1 {print $0",nome_arquivo,data_base"} NR>1' "$TMP" >> "$CSV_SAIDA"
#   rm "$TMP"
# done


# # Loop pelos arquivos que contêm "custo" no nome
# for file in ./output/credito_2025_*; do
#     echo "Convertendo $file..."
#     # in2csv --sheet "Lançamentos" --skip-lines 8 "$file" | tail -n +2 >> "$CSV_SAIDA"
#     # Extrai a data base do nome do arquivo, exemplo: custo_2025_01.xls -> 2025_01
#     data_base=$(echo "$file" | grep -oP '\d{4}_\d{2}' | awk -F'_' '{printf "01/%s/%s\n", $2, $1}')
#     # Gera CSV temporário

#     grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2},[^,]+' "$file" | awk -F',' 'BEGIN{OFS=","} { gsub(/, /,",",$0)} { if(!match($3, /[0-9].[0-9]+?$/)) { $3=$3}else{$3=","$3} ; OFS=",";print $0}' > "$TMP"

#     # in2csv --skip-lines 1 "$file" 2>/dev/null | tail -n +1  | awk -F';' 'BEGIN{OFS=","} {for(i=1;i<=NF;i++){gsub(/"/,"""",$i); $i="""$i"""}; print}' > "$TMP"
#     # Adiciona colunas extras (nome_arquivo, data_base) ao CSV, pulando o header se não for o primeiro arquivo
#     awk -v fname="$(basename "$file")" -v dbase="$data_base" 'FNR>1 {print $0","fname","dbase}' "$TMP" >> "$CSV_SAIDA"
#     #awk -F',' 'NR==1 {for(i=1;i<=NF;i++) if($i=="") $i="coluna_sem_nome_"i; print $0} NR>1' "$TMP" > "$CSV_SAIDA"
#     # rm "$TMP"
#     # awk -F';' 'BEGIN{OFS=","} {for(i=1;i<=NF;i++){gsub(/"/,"""",$i); $i="""$i"""}; print}'
# done

# sed -i -E 's/^([0-9]{4}-[0-9]{2}-[0-9]{2}),([^,]+),([^,]+|),(\-[0-9.]+|[0-9.]+)/"\1\";"\2\";"\3\";"\4\"/' "$CSV_SAIDA"

# echo "Conversão finalizada. Arquivo gerado: $CSV_SAIDA"

# aws s3 cp "$CSV_SAIDA" "$S3_BUCKET/output/credito_${ANO}.csv"

# echo "Conversão finalizada. Arquivo gerado: $S3_BUCKET/output/credito_${ANO}.csv"



# Loop pelos arquivos que contêm "custo" no nome
for file in ./output/credito_2025_*.csv; do
    

    # ./etlcreds3csv.sh $file
    
    file2="$(basename "${file%.csv}_temp.csv")"

    
    if [[ -f "./output/$file2" ]]; then
        # Gera CSV temporário
        echo "Copiar ./output/$file2 para arquivo final..."
        in2csv --skip-lines 1 "./output/$file2" 2>/dev/null \
        | awk -F',' 'BEGIN{OFS=","} NR==0{print; next} { if($1 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) { split($1,a,"-"); $1=a[3]"/"a[2]"/"a[1] } print }' \
        | awk -F',' 'BEGIN{OFS=","} NR==0{print; next} { if($6 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/) { split($6,a,"-"); $6=a[3]"/"a[2]"/"a[1] } print }' >> "./output/credito_2025.csv"
    fi
done

# rm -rf "./output/$1.csv"

# mv $FILE_CSV "./output/$1.csv"

# done

# echo "Conversão finalizada. Arquivo gerado: $CSV_SAIDA"

# S3_BUCKET_BRONZE="s3://medalion-cust/bronze"

# aws s3 cp "$CSV_SAIDA" "$S3_BUCKET_BRONZE/credito_${ANO}.csv"

# echo "Conversão finalizada. Arquivo gerado: $S3_BUCKET_BRONZE/credito_${ANO}.csv"


# # Loop pelos arquivos que contêm "custo" no nome
# for file in ./output/credito_2025_*_temp.csv; do
#     echo "Copiar $file para arquivo final..."
#     # Gera CSV temporário
#     in2csv --skip-lines 1 "$file" 2>/dev/null | tail -n +1 >> "$CSV_SAIDA"
# done

# opcional: converter delimitador para ponto-e-vírgula (requer csvkit -> csvformat)
# csvformat -D ';' "$CSV_SAIDA" > "${CSV_SAIDA%.csv}_pv.csv" && mv "${CSV_SAIDA%.csv}_pv.csv" "$CSV_SAIDA"


# # Script para unir arquivos XLS a partir da linha 9 em um único CSV
# ANO=$1
# echo "Ano recebido: $ANO"

# # Caminho do bucket S3 e prefixo dos arquivos
# S3_BUCKET="s3://medalion-cust/raw"
# # fontes originais no s3
# #s3://medalion-cust/raw/original/

# Nome do CSV de saída
# CSV_SAIDA="./output/${$1}.csv"
# TMP="./output/temp.csv"

# # Limpa o arquivo de saída se já existir
# > "$CSV_SAIDA"

# # Baixa arquivos do S3 para o diretório local
# aws s3 cp "$S3_BUCKET/original/" "./output/" --recursive --exclude "*" --include "credito_${ANO}_*.csv"

# sleep 5

# # Script para unir arquivos XLS a partir da linha 9 em um único CSV
# ANO=$1
# echo "Ano recebido: $ANO"

# docker exec -it raw-my-cust /bin/bash

# docker run -i -t cb664a0eb529 /bin/bash