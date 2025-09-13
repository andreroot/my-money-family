
import pandas as pd
import os
import datetime as dt    

import gspread
from gspread_dataframe import set_with_dataframe

# Autenticação
from credencial.credencial_gcp import my_credencial

# generate cust
from transform.transform_cust import  func_generate_sheets_type_cust, func_generate_calender, func_generate_depto_cust, func_generate_fix_type_cust

from transform.transform_receb import func_transform_receb

def connect():
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/app/.ssh/my-chave-gcp-devsamelo2.json'
    gc = gspread.service_account(filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    return gc

def generate():
    
    # Leia o CSV
    df = pd.read_csv("/app/output/extrato_2025.csv")

    # Abra a planilha pelo nome
    sh = connect().open("extrato_2025")

    worksheet = sh.worksheet("extrato")  # ou sh.worksheet("NomeDaAba")
    worksheet.clear() # Limpa a aba antes de escrever

    # Escreva no Google Sheets
    set_with_dataframe(worksheet, df)

    print(f"TOTAL DE LINHAS EXTRATO: {len(df)}", "\n")

    return df

def generate_cred():
    
    # Leia o CSV
    df = pd.read_csv("/app/output/credito_2025.csv")

    # Abra a planilha pelo nome
    sh = connect().open("extrato_2025")

    worksheet = sh.worksheet("credito")  # ou sh.worksheet("NomeDaAba")
    worksheet.clear() # Limpa a aba antes de escrever

    # Escreva no Google Sheets
    set_with_dataframe(worksheet, df)

    print(f"TOTAL DE LINHAS CREDITO: {len(df)}", "\n")

    return df

# TRANSFORMACAO DO DADOS DE SALDO 
def stop(df):

    print(f"-->SALDO / NEGATICO OU POSITIVO / STOP", "\n")

    df = df.loc[(df['descricao'].str.contains(r'SALDO[^\\b]+\w|SDO[^\\b]+CTA\/+APL[^\\b]+\w'))].copy()

    df = df.rename(columns={
        "dt_custo": "dt_receb"
    })

    # Abra a planilha pelo nome
    sh = connect().open("extrato_2025")
    worksheet = sh.worksheet("extract_saldo_transform")  # ou sh.worksheet("NomeDaAba")

    # Escreva no Google Sheets
    worksheet.clear() # Limpa a aba antes de escrever
    set_with_dataframe(worksheet, df[['dt_receb', 'descricao', 'valor_saldo', 'data_base', 'process_time']])

    print(f"TOTAL DE LINHAS STOP: {len(df)}", "\n")

# TRANSFORMACAO DO DADOS DE CUSTO 
def pushout(df):

    print(f"-->SAIDA / CUSTO / PUSHOUT", "\n")

    # definição do contexto nos dados identifcar como custo
    df = df.loc[df['valor_custo'] < 0].copy()
    df = df.loc[df['descricao'].str.contains(r'SALDO[^\\b]+\w|APL[^\\b]APLIC[^\\b]AUT[^\\b]MAIS|RES[^\\b]APLIC[^\\b]AUT[^\\b]MAIS|REND[^\\b]PAGO[^\\b]APLIC[^\\b]AUT[^\\b]MAIS')==False].copy()
    
    df["valor_custo"] = df["valor_custo"].abs()

    # transform dados + colunas
    df = func_generate_sheets_type_cust(df)
    df = func_generate_fix_type_cust(df)
    df = func_generate_calender(df)
    df = func_generate_depto_cust(df)

    # Abra a planilha pelo nome
    sh = connect().open("extrato_2025")
    worksheet = sh.worksheet("extract_cust_transform")  # ou sh.worksheet("NomeDaAba")

    # Escreva no Google Sheets
    worksheet.clear() # Limpa a aba antes de escrever
    set_with_dataframe(worksheet, df[['dt_custo', 'descricao', 'valor_custo', 'data_base', 'process_time', 'tipo_custo', 'dept_custo', 'classificacao_custo', 'area_custo']])

    print(f"TOTAL DE LINHAS PUSHOUT: {len(df)}", "\n")

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


# TRANSFORMACAO DO DADOS DE RECEBIMENTO 
def pulling(df):

    print(f"-->ENTRADA / RECEBIDOS / PULLING", "\n")

    # definição do contexto nos dados identifcar como recebido
    df = df.loc[df['valor_receb'] >= 0].copy()
    df = df.loc[df['descricao'].str.contains(r'SALDO[^\\b]+\w')==False].copy()
    df = df.loc[df['descricao'].str.contains(r'RES APLIC[^\\b]+\w')==False].copy()

    df = func_transform_receb(df)

    df = df.rename(columns={
        "dt_custo": "dt_receb"
    })

    # Abra a planilha pelo nome
    sh = connect().open("extrato_2025")
    worksheet = sh.worksheet("extract_receb_transform")  # ou sh.worksheet("NomeDaAba")

    # Escreva no Google Sheets
    worksheet.clear() # Limpa a aba antes de escrever
    set_with_dataframe(worksheet, df[['dt_receb', 'descricao',  'valor_receb', 'data_base', 'process_time', 'tipo_receb']])
    
    print(f"TOTAL DE LINHAS PULLING: {len(df)}", "\n")


def generate_type_cust_none(df):


    # definição do contexto nos dados identifcar como custo
    df = df.loc[df['valor_custo'] < 0].copy()
    df = df.loc[df['descricao'].str.contains(r'SALDO[^\\b]+\w|APL[^\\b]APLIC[^\\b]AUT[^\\b]MAIS|RES[^\\b]APLIC[^\\b]AUT[^\\b]MAIS|REND[^\\b]PAGO[^\\b]APLIC[^\\b]AUT[^\\b]MAIS')==False].copy()
    

    # transform dados + colunas
    df = func_generate_sheets_type_cust(df)
    df = func_generate_fix_type_cust(df)


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

    return df



# TRANSFORMACAO DO DADOS DE CUSTO 
def pushout_cred(df):

    print(f"-->SAIDA / CREDITO / PUSHOUT", "\n")


    # transform dados + colunas
    df = func_generate_sheets_type_cust(df)
    df = func_generate_fix_type_cust(df)
    # df = func_generate_calender(df)
    df = func_generate_depto_cust(df)

    df = df.rename(columns={
        "tipo_custo": "tipo_credito",
        "dept_custo": "dept_credito",
        "classificacao_custo": "classificacao_credito",
        "area_custo": "area_credito"

    })

    # Abra a planilha pelo nome
    sh = connect().open("extrato_2025")
    worksheet = sh.worksheet("extract_cred_transform")  # ou sh.worksheet("NomeDaAba")

    # Escreva no Google Sheets
    worksheet.clear() # Limpa a aba antes de escrever
    set_with_dataframe(worksheet, df[["dt_credito", "descricao", "valor_credito", "data_base", "process_time", "tipo_credito", "dept_credito", "classificacao_credito", "area_credito"]])

    print(f"TOTAL DE LINHAS PUSHOUT: {len(df)}", "\n")

    if len(df[(df["tipo_credito"].isnull())])==0:
        print(f"-->SAIDA NULO / CREDITO NULO / PUSHOUT", "\n")
        print("NENHUM TIPO DE CUSTO NULO PARA GERAR!!!")
        return df
    else:
        print(f'Total tipos_encontrados:{len(df[(df["tipo_credito"].notnull())])} / tipos_nao_encontrados:{len(df[(df["tipo_credito"].isnull())])}\n')

    # Abra a planilha pelo nome
    sh = connect().open("depara_tipo_custo")

    worksheet = sh.worksheet("depara_nulo")  # ou sh.worksheet("NomeDaAba")
    worksheet.clear() # Limpa a aba antes de escrever

    # Escreva no Google Sheets
    set_with_dataframe(worksheet, df[(df["tipo_credito"].isnull())][["descricao"]])

    return df