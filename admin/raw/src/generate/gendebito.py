import pandas as pd
import os
import sys
import datetime as dt    


# csv gerado local ./output
from getdata.get_csv import read_csv_debito
from getdata.get_parquet import get_parquet_silver

# model
from model.model import model_data

#gerar parquet apartir do csv
from generate.generate_parquet import generate_parquet_debito
from generate.generate_sheets import generate_analytics_sheets_deb

class Generate:
    
    def __init__(self):
        pass
    
    def main_debito(self, ano): 

        # generate
        df = pd.DataFrame()

        df = read_csv_debito(ano)
        #df = get_parquet_silver(f'extrato_{ano}','debito')

        # print(df.columns)

        print("--> GERAR PARQUET AWS RAW", "\n")
        generate_parquet_debito(df,ano)

        print("--> GERAR SHEETS GOOGLE RAW", "\n")
        generate_analytics_sheets_deb(df, ano, "extrato")

        dfd = model_data(df)
        
        if dfd is not None:
            dfd["valor_custo"] = dfd["valor_custo"].fillna(0)
            dfd["valor_saldo"] = dfd["valor_saldo"].fillna(0)
            # ... resto do processamento ...
        else:
            print("Erro ao validar dados, DataFrame não foi criado.")    

        now = dt.datetime.now()
        dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

        dfd["process_time"] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")

        return dfd
    

