import pandas as pd
import os
import sys
import datetime as dt    
import subprocess

def read_csv_union_credito(ano):
    
    # Leia o CSV
    # df = pd.read_csv(f"./output/credito_{ano}.csv")

    # Caminho e nome do arquivo no S3
    s3_path1 = f"s3://medalion-cust/bronze/credito_uniclass_signature_{ano}.csv"
    local_path1 = f"./output/credito_uniclass_signature_{ano}.csv"

    s3_path2 = f"s3://medalion-cust/bronze/credito_uniclass_black_{ano}.csv"
    local_path2 = f"./output/credito_uniclass_black_{ano}.csv"

    # Baixa os arquivos do S3 se não existirem localmente
    if not os.path.exists(local_path1):
        subprocess.run(["aws", "s3", "cp", s3_path1, local_path1], check=True)
    if not os.path.exists(local_path2):
        subprocess.run(["aws", "s3", "cp", s3_path2, local_path2], check=True)

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

    return df



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

    return df