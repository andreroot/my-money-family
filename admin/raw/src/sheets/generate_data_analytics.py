
import pandas as pd
import os
import datetime as dt    

# generate cust
from transform.transform_cust import  func_generate_sheets_type_cust, func_generate_calender, func_generate_depto_cust, func_generate_fix_type_cust

from transform.transform_receb import func_transform_receb

from parquet.generate_parquet import generate_parquet_analytics
from sheets.generate_sheets import generate_analytics_sheets_stop, generate_analytics_sheets_pushout, generate_analytics_sheets_pushout_cred, generate_analytics_sheets_pulling


# TRANSFORMACAO DO DADOS DE SALDO 
def stop(df, ano):

    print(f"-->SALDO / NEGATICO OU POSITIVO / STOP", "\n")

    df = df.loc[(df['descricao'].str.contains(r'SALDO[^\\b]+\w|SDO[^\\b]+CTA\/+APL[^\\b]+\w'))].copy()

    df = df.rename(columns={
        "dt_custo": "dt_receb"
    })

    generate_parquet_analytics(df, 'stop_saldo', f'extract_saldo_transform_{ano}')

    # Abra a planilha pelo nome
    generate_analytics_sheets_stop(df[['dt_receb', 'descricao', 'valor_saldo', 'data_base', 'process_time']], 
                                ano, 
                                'extract_saldo_transform')

# TRANSFORMACAO DO DADOS DE CUSTO 
def pushout(df, ano):

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

    # Rename columns for consistency
    generate_parquet_analytics(df, 'pushout_saida', f'extract_cust_transform_{ano}')

    # Abra a planilha pelo nome
    generate_analytics_sheets_pushout(df[['dt_custo', 'descricao', 'valor_custo', 'data_base', 'process_time', 'tipo_custo', 'dept_custo', 'classificacao_custo', 'area_custo']], 
                                        ano, 
                                        'extract_cust_transform')

# TRANSFORMACAO DO DADOS DE RECEBIMENTO 
def pulling(df, ano):

    print(f"-->ENTRADA / RECEBIDOS / PULLING", "\n")

    # definição do contexto nos dados identifcar como recebido
    df = df.loc[df['valor_receb'] >= 0].copy()
    df = df.loc[df['descricao'].str.contains(r'SALDO[^\\b]+\w')==False].copy()
    df = df.loc[df['descricao'].str.contains(r'RES APLIC[^\\b]+\w')==False].copy()

    df = func_transform_receb(df)

    df = df.rename(columns={
        "dt_custo": "dt_receb"
    })

    generate_parquet_analytics(df, 'pulling_entrada', f'extract_receb_transform_{ano}')

    # Abra a planilha pelo nome
    generate_analytics_sheets_pulling(df[['dt_receb', 'descricao',  'valor_receb', 'data_base', 'process_time', 'tipo_receb']], 
                                        ano, 
                                        'extract_receb_transform')

# TRANSFORMACAO DO DADOS DE CUSTO 
def pushout_cred(df, ano):

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

    generate_parquet_analytics(df, ano, f'extract_cred_transform_{ano}')

    # Abra a planilha pelo nome
    generate_analytics_sheets_pushout_cred(df[["dt_credito", "descricao", "valor_credito", "data_base", "process_time", "tipo_credito", "dept_credito", "classificacao_credito", "area_credito"]], 
                                            ano, 
                                            'extract_cred_transform')



