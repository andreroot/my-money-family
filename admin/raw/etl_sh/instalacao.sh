#!/bin/bash
# Instalação completa do csvkit + dependências para Excel

echo "📦 Atualizando pacotes..."
# sudo apt update -y
# sudo apt install -y python3 python3-pip

source /home/andre/projetos/my-money-family/.venv/bin/activate

echo "📦 Instalando csvkit..."
pip3 install --upgrade pip
pip3 install csvkit==1.0.7

echo "📦 Instalando bibliotecas para leitura de Excel (.xls e .xlsx)..."
# xlrd 1.2.0 é a última versão que ainda suporta XLS (Excel 97-2003)
pip3 install xlrd==1.2.0 openpyxl

echo "✅ Instalação concluída!"
echo "Versão do in2csv:"
in2csv --version

echo "📦 Instalando bibliotecas para gerar sheets no google sheets online..."
# xlrd 1.2.0 é a última versão que ainda suporta XLS (Excel 97-2003)
pip install gspread pandas gspread_dataframe

echo "✅ Instalação concluída!"