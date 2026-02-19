
# Loop pelos arquivos que contêm "custo" no nome
for file in ./output/credito_${ANO}_*.xls; do

  echo "Convertendo XLS $file..."

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

  sleep 5
done

