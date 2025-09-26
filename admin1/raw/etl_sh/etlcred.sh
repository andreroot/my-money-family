#!/bin/bash
# Script para unir arquivos XLS a partir da linha 9 em um único CSV
# sudo apt install csvkit

# Verifica se pelo menos 1 arquivo foi passado
# /mnt/c/Users/andre/Documents/github/my-money-family/admin/etl/excel/custo_2025_01.xls
#!/bin/bash

# Diretório onde estão os arquivos (ajuste se necessário)
DIR="/mnt/c/Users/andre/Documents/github/my-money-family/admin/etl/excel/"

# Nome do CSV de saída
CSV_SAIDA="/home/andre/projetos/my-money-family/admin/raw/output/extrato_cred_2025.csv"

# temp
TMP="/home/andre/projetos/my-money-family/admin/raw/output/temp.csv"


# Limpa o arquivo de saída se já existir
> "$CSV_SAIDA"

# if [ "$#" -lt 1 ]; then
#   echo "Uso: $0 arquivo1.xls [arquivo2.xls ...]"
#   exit 1
# fi

# Loop pelos arquivos que contêm "custo" no nome
for file in "$DIR"*credito_2025_01*; do
  if [[ -f "$file" ]]; then
    echo "Pegar os nomes de colunas do primeiro arquivo $file..."
    # Pega o cabeçalho só do primeiro arquivo
    #in2csv --sheet "Lançamentos" --skip-lines 1 "$file" | head -n 1 > "$TMP"
    in2csv --sheet "Lançamentos" --skip-lines 1 "$file" | head -n 1 | awk -F',' 'BEGIN{OFS=","} {for(i=1;i<=NF;i++) if($i=="c") $i="coluna_sem_nome_"i; OFS=",";print $0}' > "$TMP"
    awk 'BEGIN{OFS=","} NR==1 {print $0",nome_arquivo,data_base"} NR>1' "$TMP" >> "$CSV_SAIDA"
    rm "$TMP"
  fi
done


# Loop pelos arquivos que contêm "custo" no nome
for file in "$DIR"*credito*; do
  if [[ -f "$file" ]]; then
    echo "Convertendo $file..."
    # in2csv --sheet "Lançamentos" --skip-lines 8 "$file" | tail -n +2 >> "$CSV_SAIDA"
    # Extrai a data base do nome do arquivo, exemplo: custo_2025_01.xls -> 2025_01
    data_base=$(echo "$file" | grep -oP '\d{4}_\d{2}' | awk -F'_' '{printf "01/%s/%s\n", $2, $1}')
    # Gera CSV temporário
    in2csv --sheet "Lançamentos" --skip-lines 1 "$file" | tail -n +1 > "$TMP"
    # Adiciona colunas extras (nome_arquivo, data_base) ao CSV, pulando o header se não for o primeiro arquivo
    awk -v fname="$(basename "$file")" -v dbase="$data_base" 'FNR>1 {print $0","fname","dbase}' "$TMP" >> "$CSV_SAIDA"
    #awk -F',' 'NR==1 {for(i=1;i<=NF;i++) if($i=="") $i="coluna_sem_nome_"i; print $0} NR>1' "$TMP" > "$CSV_SAIDA"
    rm "$TMP"
  fi
done

echo "Conversão finalizada. Arquivo gerado: $CSV_SAIDA"



# # Para cada arquivo, pula as 8 primeiras linhas e adiciona ao CSV final
# for f in "$@"; do
#   echo "Processando $f ..."
#   #in2csv "$f" | tail -n +9 >> "$OUTPUT"
#   in2csv --sheet "Lançamentos" --skip-lines 8 
# done

# echo "✅ Arquivo final gerado: $OUTPUT"
