
import pandas as pd
import os
import datetime as dt    

import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# Autenticação
from credencial.credencial_gcp import my_credencial


def connect():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/andre/.ssh/my-chave-gcp-devsamelo2.json'
    gc = gspread.service_account(filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    return gc

def generate_process(sheet_name="extrato_2025", tab_name="extract_cust_transform"):
 
    # Abra a planilha pelo nome
    sh = connect().open(sheet_name)

    worksheet = sh.worksheet(tab_name)  # ou sh.worksheet("NomeDaAba")

    # Lê os dados como DataFrame
    df = get_as_dataframe(worksheet, evaluate_formulas=True)

    print(f"TOTAL DE LINHAS EXTRATO: {len(df)}", "\n")

    return df

def analytics_sheets(df):
    # Abra a planilha pelo nome
    sh = connect().open("resumo financeiro")

    worksheet = sh.worksheet("previsao_plano_financeiro")  # ou sh.worksheet("NomeDaAba")
    worksheet.clear() # Limpa a aba antes de escrever

    # Escreva no Google Sheets
    set_with_dataframe(worksheet, df)

    print(f"TOTAL DE LINHAS GERADAS NO SHEETS: {len(df)}", "\n")

    return df