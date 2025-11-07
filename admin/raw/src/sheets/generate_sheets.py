
import pandas as pd
import os
import datetime as dt    

import gspread
from gspread_dataframe import set_with_dataframe

# Autenticação
from credencial.credencial_gcp import my_credencial


def connect():
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/app/.ssh/my-chave-gcp-devsamelo2.json'
    gc = gspread.service_account(filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    return gc

def generate_analytics_sheets_stop(df, ano, aba):

    # Abra a planilha pelo nome
    sh = connect().open(f"extrato_{ano}")
    worksheet = sh.worksheet(aba)  # ou sh.worksheet("NomeDaAba")

    # Escreva no Google Sheets
    worksheet.clear() # Limpa a aba antes de escrever
    set_with_dataframe(worksheet, df)

    print(f"TOTAL DE LINHAS STOP: {len(df)}", "\n")

def generate_analytics_sheets_pushout(df, ano, aba):

    # Abra a planilha pelo nome
    sh = connect().open(f"extrato_{ano}")
    worksheet = sh.worksheet(aba)  # ou sh.worksheet("NomeDaAba")

    # Escreva no Google Sheets
    worksheet.clear() # Limpa a aba antes de escrever
    set_with_dataframe(worksheet, df)

    print(f"TOTAL DE LINHAS PUSHOUT: {len(df)}", "\n")

    generate_depara_tipo_custo(df, "debito")

def generate_analytics_sheets_pulling(df, ano, aba):

    # Abra a planilha pelo nome
    sh = connect().open(f"extrato_{ano}")
    worksheet = sh.worksheet("extract_receb_transform")  # ou sh.worksheet("NomeDaAba")

    # Escreva no Google Sheets
    worksheet.clear() # Limpa a aba antes de escrever
    set_with_dataframe(worksheet, df)
    
    print(f"TOTAL DE LINHAS PULLING: {len(df)}", "\n")

def generate_analytics_sheets_pushout_cred(df, ano, aba):

    # Abra a planilha pelo nome
    sh = connect().open(f"extrato_{ano}")
    worksheet = sh.worksheet(aba)  # ou sh.worksheet("NomeDaAba")

    # Escreva no Google Sheets
    worksheet.clear() # Limpa a aba antes de escrever
    set_with_dataframe(worksheet, df)

    print(f"TOTAL DE LINHAS PUSHOUT/CRED: {len(df)}", "\n")

    generate_depara_tipo_custo(df, "credito")

def generate_depara_tipo_custo(df, tipo_docu):

    if tipo_docu == "credito":
        df = df.rename(columns={"descricao": "tipo_custo"})

    if len(df[(df["tipo_custo"].isnull())])==0:
        print(f"-->SAIDA NULO / CUSTO NULO / PULLING", "\n")
        print("NENHUM TIPO DE CUSTO NULO PARA GERAR!!!")
        return df
    else:
        print(f'Total tipos_encontrados:{len(df[(df["tipo_custo"].notnull())])} / tipos_nao_encontrados:{len(df[(df["tipo_custo"].isnull())])}\n')


    # Abra a planilha pelo nome
    sh = connect().open("depara_tipo_custo")

    worksheet = sh.worksheet("depara_nulo")  # ou sh.worksheet("NomeDaAba")
    worksheet.clear() # Limpa a aba antes de escrever

    # Escreva no Google Sheets
    set_with_dataframe(worksheet, df[(df["tipo_custo"].isnull())][["descricao"]])
