import pandas as pd
import os
import sys
import datetime as dt    

from concurrent.futures import ProcessPoolExecutor

# model
from model.model import model_data
from model.modelcred import model_data as model_data_cred

# generate 
from sheets.generate_data import generate, generate_cred, generate_type_cust_none, pushout, pulling, stop, pushout_cred

class Generate:
    
    def __init__(self):
        pass
    
    def main_debito(self, ano): 

        # generate
        df = pd.DataFrame()

        df = generate(ano)

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
        print(dfc.head())
        dfc = model_data_cred(dfc)

        now = dt.datetime.now()
        dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

        dfc["process_time"] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")
        
        return dfc

if __name__=='__main__':

    m = Generate()
    ano = sys.argv[1]

    print(f"Ano recebido no main: {ano}")

    # debito: custo, receber, saldo
    dfd = pd.DataFrame()
    dfd = m.main_debito(ano)

    # credito
    dfc = pd.DataFrame()
    dfc = m.main_credito(ano)

    # tasks = [pushout(dfd, ano), pulling(dfd, ano), stop(dfd, ano), pushout_cred(dfc, ano)]

    with ProcessPoolExecutor(max_workers=4)  as executor:
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