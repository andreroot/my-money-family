import pandas as pd
import os
import sys
import datetime as dt    

from concurrent.futures import ThreadPoolExecutor

# gerar arquivos parquet baseados nos arquivos csv
from generate.generate_data_analytics import stop, pushout, pushout_cred, pulling

from generate.gendebito import Generate as GenerateDeb
from generate.gencredito import Generate as GenerateCred

if __name__=='__main__':


    ano = sys.argv[1]
    type_doc = sys.argv[2]

    print(f"Ano recebido no main: {ano}")
    print(f"Tipo de extrato recebido na body: {type_doc}")
    
    if type_doc=='debito':

        # debito: custo, receber, saldo
        dfd = pd.DataFrame()
        
        # instancia classe gera dados apartior de csv ou parquet - local ou na aws
        md = GenerateDeb()
        dfd = md.main_debito(ano)

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

        # instancia classe gera dados apartior de csv ou parquet - local ou na aws
        mc = GenerateCred()

        if ano=='2026':
            dfc = mc.main_credito(ano, True)
        else:
            dfc = mc.main_credito(ano, False)

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
        md = GenerateDeb()
        dfd = pd.DataFrame()
        dfd = md.main_debito(ano)

        # credito
        mc = GenerateCred()
        dfc = pd.DataFrame()
        dfc = mc.main_credito(ano)

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