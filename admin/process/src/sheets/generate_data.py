
import pandas as pd
import os
import datetime as dt    

import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe

# Autenticação
from credencial.credencial_gcp import my_credencial


def connect():
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

def analytics_sheets(df, sheets="resumo financeiro", tab="previsao_plano_financeiro"):
    # Abra a planilha pelo nome
    sh = connect().open(sheets)

    worksheet = sh.worksheet(tab)  # ou sh.worksheet("NomeDaAba")
    worksheet.clear() # Limpa a aba antes de escrever

    # Escreva no Google Sheets
    set_with_dataframe(worksheet, df)

    print(f"TOTAL DE LINHAS GERADAS NO SHEETS: {len(df)}", "\n")


def analytics_sheets_param(df, sheets="resumo financeiro", tab="custo_extract_all"):
    # Abra a planilha pelo nome
    sh = connect().open(sheets)

    worksheet = sh.worksheet(tab)  # ou sh.worksheet("NomeDaAba")
    worksheet.clear() # Limpa a aba antes de escrever

    # Escreva no Google Sheets
    set_with_dataframe(worksheet, df)

    print(f"TOTAL DE LINHAS GERADAS NO SHEETS: {len(df)}", "\n")
