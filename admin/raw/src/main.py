import pandas as pd
import os
import sys
import datetime as dt    

from concurrent.futures import ThreadPoolExecutor

# model
from model.model import model_data
from model.modelcred import model_data as model_data_cred

# generate 
from sheets.get_data import read_parquet_debito as generate_debito
from sheets.get_data import read_parquet_credito as generate_cred
from sheets.generate_data_analytics import stop, pushout, pushout_cred, pulling

class Generate:
    
    def __init__(self):
        pass
    
    def main_debito(self, ano): 

        # generate
        df = pd.DataFrame()

        df = generate_debito(ano)

        df = model_data(df)

        df["valor_custo"] = df["valor_custo"].fillna(0)
        df["valor_saldo"] = df["valor_saldo"].fillna(0)

        now = dt.datetime.now()
        dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

        df["process_time"] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")

        return df
    
    def main_credito(self, ano):

        # generate
        dfc = pd.DataFrame()

        dfc = generate_cred(ano)

        dfc = model_data_cred(dfc)

        now = dt.datetime.now()
        dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

        dfc["process_time"] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")
        
        return dfc

if __name__=='__main__':

    m = Generate()
    ano = sys.argv[1]

    type_doc = sys.argv[2]

    print(f"Ano recebido no main: {ano}")
    print(f"Tipo de extrato recebido na body: {type_doc}")

    if type_doc=='debito':

        # debito: custo, receber, saldo
        dfd = pd.DataFrame()
        
        dfd = m.main_debito(ano)

        # dfd = generate_debito(ano)

        # tasks = [pushout(dfd, ano), pulling(dfd, ano), stop(dfd, ano), pushout_cred(dfc, ano)]

        with ThreadPoolExecutor(max_workers=3)  as executor:
            # results = executor.map(lambda f: f.result(), tasks)
            # for result in results:
            #     print(result)
            futures = [
                executor.submit(pushout, dfd, ano),
                executor.submit(pulling, dfd, ano),
                executor.submit(stop, dfd, ano)
            ]
            for future in futures:
                print(future.result())

    elif type_doc=='credito':
        
        # credito
        dfc = pd.DataFrame()
        dfc = m.main_credito(ano)

        # tasks = [pushout(dfd, ano), pulling(dfd, ano), stop(dfd, ano), pushout_cred(dfc, ano)]

        with ThreadPoolExecutor(max_workers=4)  as executor:
            # results = executor.map(lambda f: f.result(), tasks)
            # for result in results:
            #     print(result)
            futures = [
                executor.submit(pushout_cred, dfc, ano)
            ]
            for future in futures:
                print(future.result())

    else:

        # debito: custo, receber, saldo
        dfd = pd.DataFrame()
        dfd = m.main_debito(ano)

        # credito
        dfc = pd.DataFrame()
        dfc = m.main_credito(ano)

        # tasks = [pushout(dfd, ano), pulling(dfd, ano), stop(dfd, ano), pushout_cred(dfc, ano)]

        with ThreadPoolExecutor(max_workers=4)  as executor:
            # results = executor.map(lambda f: f.result(), tasks)
            # for result in results:
            #     print(result)
            futures = [
                executor.submit(pushout, dfd, ano),
                executor.submit(pulling, dfd, ano),
                executor.submit(stop, dfd, ano),
                executor.submit(pushout_cred, dfc, ano)
            ]
            for future in futures:
                print(future.result())