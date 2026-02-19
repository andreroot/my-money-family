
import pandas as pd
import os
import datetime as dt    
#gerar parquet apartir do csv
from parquet.generate_parquet import generate_parquet_credito, generate_parquet_debito
#ler paqruqte gerado
from parquet.get_parquet import get_parquet_process
from sheets.generate_sheets import generate_analytics_sheets_deb, generate_analytics_sheets_cred

def read_csv_debito(ano):
    
    # Leia o CSV
    # /app/output/
    # local: /home/andre/projetos/my-money-family/admin/raw/output/
    # df = pd.read_csv(f"./output/extrato_{ano}.csv")

    df = pd.read_csv(f"./output/extrato_{ano}.csv",
        sep=',',
        encoding='utf-8',
        parse_dates=['data_base'],
        dayfirst=True,
        # infer_datetime_format=True,
        keep_default_na=False
        )

    # print(df.columns)

    print("--> GERAR PARQUET AWS", "\n")
    generate_parquet_debito(df,ano)

    print("--> GERAR SHEETS GOOGLE", "\n")
    generate_analytics_sheets_deb(df, ano, "extrato")

    return df


def read_csv_union_credito(ano):
    
    # Leia o CSV
    # df = pd.read_csv(f"./output/credito_{ano}.csv")

    # v2 - leitura de 2 csv e unir num unico df
    # df1 - uniclass_signature 
    df1 = pd.read_csv(f"./output/credito_uniclass_signature_{ano}.csv",
        sep=';',
        quotechar='"',
        encoding='utf-8',
        parse_dates=['data_base'],
        dayfirst=True,
        # infer_datetime_format=True,
        keep_default_na=False
        )
    # df2 - uniclass_black
    df2 = pd.read_csv(f"./output/credito_uniclass_black_{ano}.csv",
        sep=';',
        quotechar='"',
        encoding='utf-8',
        parse_dates=['data_base'],
        dayfirst=True,
        # infer_datetime_format=True,
        keep_default_na=False
        )

    # unir datframes 
    df = pd.concat([df1, df2], ignore_index=True)

    return df

def read_csv_credito(ano, flag_unir):
    
    # Leia o CSV
    # df = pd.read_csv(f"./output/credito_{ano}.csv")

    # v2 - leitura de 2 csv 
    if flag_unir:
        df = read_csv_union_credito(ano)
    else:
        df = pd.read_csv(f"./output/credito_{ano}.csv",
            sep=';',
            quotechar='"',
            encoding='utf-8',
            parse_dates=['data_base'],
            dayfirst=True,
            # infer_datetime_format=True,
            keep_default_na=False
            )

    print("--> GERAR PARQUET AWS", "\n")
    generate_parquet_credito(df,ano)

    print("--> GERAR SHEETS GOOGLE", "\n")
    generate_analytics_sheets_cred(df, ano, "credito")

    return df



def read_parquet_debito(ano):

    df = get_parquet_process(parquet_file=f"extrato_{ano}", tipo_docu="debito", s3_options={})

    print(df.columns)

    return df

def read_parquet_credito(ano):

    df = get_parquet_process(parquet_file=f"credito_{ano}", tipo_docu="credito", s3_options={})

    print(df.columns)

    return df

