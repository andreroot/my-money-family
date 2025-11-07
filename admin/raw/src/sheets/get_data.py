
import pandas as pd
import os
import datetime as dt    

from parquet.get_parquet import get_parquet_process


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
        infer_datetime_format=True,
        keep_default_na=False
        )

    print(df.columns)

    return df

def read_csv_credito(ano):
    
    # Leia o CSV
    # df = pd.read_csv(f"./output/credito_{ano}.csv")

    df = pd.read_csv(f"./output/credito_{ano}.csv",
        sep=';',
        quotechar='"',
        encoding='utf-8',
        parse_dates=['data_base'],
        dayfirst=True,
        infer_datetime_format=True,
        keep_default_na=False
        )

    print(df.columns)

    return df


def read_parquet_debito(ano):

    df = get_parquet_process(parquet_file=f"extrato_{ano}", tipo_docu="debito", s3_options={})

    print(df.columns)

    return df

def read_parquet_credito(ano):

    df = get_parquet_process(parquet_file=f"credito_{ano}", tipo_docu="credito", s3_options={})

    print(df.columns)

    return df

