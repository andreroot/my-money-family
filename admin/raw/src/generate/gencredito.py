import pandas as pd
import os
import sys
import datetime as dt    

# csv gerado local ./output
from getdata.get_csv import read_csv_credito

# model
from model.modelcred import model_data as model_data_cred

#gerar parquet apartir do csv
from generate.generate_parquet import generate_parquet_credito
from generate.generate_sheets import generate_analytics_sheets_cred



class Generate:
    
    def __init__(self):
        pass

    def main_credito(self, ano, flag_unir):

        # generate
        dfc = pd.DataFrame()

        dfc = read_csv_credito(ano, flag_unir)

        print("--> GERAR PARQUET AWS RAW", "\n")
        generate_parquet_credito(dfc,ano)

        print("--> GERAR SHEETS GOOGLE RAW", "\n")
        generate_analytics_sheets_cred(dfc, ano, "credito")

        dfc = model_data_cred(dfc)

        now = dt.datetime.now()
        dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

        dfc["process_time"] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")



        return dfc