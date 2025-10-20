#!/bin/bash
set -e

ANO=$1
echo "Ano recebido: $ANO"

# Instalação completa do csvkit + dependências para Excel
echo "🤖 Processo Medallion - RAW - Início da instalação 🐋"

echo "🐧 Sheel Script - RAW Debito"
## EXECUTE DEBITO S3
./etl_sh/etldebs3.sh $ANO

echo "🐧 Sheel Script - RAW Credito"
## EXECUTE CREDITO S3
./etl_sh/etlcreds3.sh $ANO

echo "🐼 Execução python do ETL RAW"
python3 /app/src/main.py $ANO


# echo "🤖 Criação/Ativação da virtualenv para execução do processo RAW"

# # instala virtualenv se não estiver instalado
# pip3 install virtualenv

# # Cria o virtualenv (pode ser .venv ou outro nome)
# # Ativar virtualenv (se você realmente quiser virtualenv dentro do container)
# if [ ! -d "venv" ]; then
#   echo "📦 Criando virtualenv..."
#   python3 -m venv venv
# fi

# # Ativa o virtualenv
# source venv/bin/activate

# echo "🚀 Instalando pacotes com requirements.txt..."
# pip3 install -r /app/requirements.txt

# echo "🚀 Instalando csvkit..."

# pip3 install --upgrade pip
# pip3 install csvkit==1.0.7

# echo "🚀 Instalando bibliotecas para leitura de Excel (.xls e .xlsx)..."
# # xlrd 1.2.0 é a última versão que ainda suporta XLS (Excel 97-2003)
# pip3 install xlrd==1.2.0 openpyxl

# echo "Versão do in2csv:"
# in2csv --version

# echo "🚀 Instalando bibliotecas para gerar sheets no google sheets online..."
# pip3 install gspread pandas gspread_dataframe

# echo "🚦 Instalação concluída! 🐋"

# echo "🐧 Sheel Script - RAW Debito"

# # chown -R appuser:appgroup /app/output/original
# # chmod -R 775 /app/output/original

# ## EXECUTE DEBITO S3
# ./etl_sh/etldebs3.sh

# echo "🐧 Sheel Script - RAW Credito"
# ## EXECUTE CREDITO S3
# ./etl_sh/etlcreds3.sh

# echo "🐼 Execução python do ETL RAW"
# python3 /app/src/main.py
#!/bin/bash -c 'for file in /app/output/original/custo_2025_*.xls; do if [[ -f "$file" ]]; then echo "Convertendo $file..." ; fi ;done'