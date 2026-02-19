import pandas as pd
import os
import sys
import datetime as dt    

from concurrent.futures import ThreadPoolExecutor

# model
from model.model import model_data
from model.modelcred import model_data as model_data_cred

# generate ler arquivo local csv ou sheets
from getdata.get_data import read_csv_debito as generate_debito
from getdata.get_data import read_csv_credito as generate_cred

# gerar arquivos parquet baseados nos arquivos csv
from sheets.generate_data_analytics import stop, pushout, pushout_cred, pulling

class Generate:
    
    def __init__(self):
        pass
    
    def main_debito(self, ano): 

        # generate
        df = pd.DataFrame()

        df = generate_debito(ano)

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
    
    def main_credito(self, ano, flag_unir):

        # generate
        dfc = pd.DataFrame()

        dfc = generate_cred(ano, flag_unir)

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
            for i, future in enumerate(futures, 1):
                status = "running" if future.running() else "done" if future.done() else "pending"
                print(f"Tarefa {i}: status = {status}", "\n")
                if future.done():
                    if future.exception():
                        print(f"Tarefa {i}: erro -> {future.exception()}", "\n")

    elif type_doc=='credito':
        
        
        # credito
        dfc = pd.DataFrame()
        if ano=='2026':
            dfc = m.main_credito(ano, True)
        else:
            dfc = m.main_credito(ano, False)

        # tasks = [pushout(dfd, ano), pulling(dfd, ano), stop(dfd, ano), pushout_cred(dfc, ano)]

        with ThreadPoolExecutor(max_workers=4)  as executor:
            # results = executor.map(lambda f: f.result(), tasks)
            # for result in results:
            #     print(result)
            futures = [
                executor.submit(pushout_cred, dfc, ano)
            ]
            for i, future in enumerate(futures, 1):
                status = "running" if future.running() else "done" if future.done() else "pending"
                print(f"Tarefa {i}: status = {status}", "\n")
                if future.done():
                    if future.exception():
                        print(f"Tarefa {i}: erro -> {future.exception()}", "\n")

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
            for i, future in enumerate(futures, 1):
                status = "running" if future.running() else "done" if future.done() else "pending"
                print(f"Tarefa {i}: status = {status}", "\n")
                if future.done():
                    if future.exception():
                        print(f"Tarefa {i}: erro -> {future.exception()}", "\n")