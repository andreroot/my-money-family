
import pandas as pd
import os
import datetime as dt    

#ler parquet gerado
from getdata.get_parquet import get_parquet_silver
from getdata.get_sheets import get_depara_tipo_custo

def read_parquet_debito(ano):

    df = get_parquet_silver(parquet_file=f"extrato_{ano}", tipo_docu="debito", s3_options={})

    print(df.columns)

    return df

def read_parquet_credito(ano):

    df = get_parquet_silver(parquet_file=f"credito_{ano}", tipo_docu="credito", s3_options={})

    print(df.columns)

    return df

def read_sheest():

    list = get_depara_tipo_custo()

    return list