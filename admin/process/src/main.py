import os
import sys
import time
import datetime
import pandas as pd

from transform.mycalculateanalytics import calculate_analytics_sheets
from transform.myexplorecustanalytics import explore_cust_analytics_sheets

from parquet.get_parquet import get_parquet_process as getdata

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import time

def tasks_old():

    tasks = [("extrato_2025", "extract_cust_transform"), ("extrato_2025", "extract_saldo_transform"), ("extrato_2025", "extract_receb_transform")]

    results = {}  # Ou use uma lista se preferir

    with ProcessPoolExecutor(max_workers=3)  as executor:
        #results = executor.map(tasks)
        futures = {executor.submit(getdata, sheet_name, tab_name): tab_name for sheet_name, tab_name in tasks}
        
        for future in as_completed(futures):
            tab_name = futures[future]
            try:
                df = future.result()
                results[tab_name] = df  # Salva o DataFrame com a chave do tab_name
                print(f"Processado: {tab_name}, shape: {df.shape}")
            except Exception as e:
                print(f"Erro ao processar {tab_name}: {e}")

                                
        executor.shutdown(wait=True)


def tasks(ano):

    df = pd.DataFrame()

    tasks = [("analytics_pulling_entrada",f"extract_receb_transform_{ano}"), 
            ("analytics_pushout_saida",f"extract_cust_transform_{ano}"), 
            ("analytics_stop_saldo",f"extract_saldo_transform_{ano}")]

    results = {}  # Ou use uma lista se preferir

    with ProcessPoolExecutor(max_workers=3)  as executor:
        #results = executor.map(tasks)
        futures = {executor.submit(getdata, tipo_docu, tab_name): tab_name for tipo_docu, tab_name in tasks}
        
        for future in as_completed(futures):
            tab_name = futures[future]
            try:
                df = future.result()
                results[tab_name] = df  # Salva o DataFrame com a chave do tab_name
                print(f"Processado: {tab_name}, shape: {df.shape}")
            except Exception as e:
                print(f"Erro ao processar {tab_name}: {e}")

                                
        executor.shutdown(wait=True)
    

    # Agora você pode acessar cada DataFrame pelo nome da aba:
    dfc = results.get(f"extract_cust_transform_{ano}")
    dfs = results.get(f"extract_saldo_transform_{ano}")
    dfr = results.get(f"extract_receb_transform_{ano}")

    # TRASNFORMAÇÃO DEBITO - CUSTO
    calculate_analytics_sheets(ano, dfs, dfc, dfr)
    explore_cust_analytics_sheets(ano, dfc)

    # 

    # with ThreadPoolExecutor(max_workers=2)  as executor:
    #     task1=[calculate_analytics_sheets(dfs, dfc, dfr), extract_analytics_sheets(dfc)]
    #     results = executor.map(tasks)
    
if __name__=='__main__':


    # dfc = generate(sheet_name="extrato_2025", tab_name="extract_cust_transform")
    # dfs = generate(sheet_name="extrato_2025", tab_name="extract_saldo_transform")
    # dfr = generate(sheet_name="extrato_2025", tab_name="extract_receb_transform")

    ano = sys.argv[1]

    msg = f" Finance | 1/3 | Preparar dados para processamento, listagem de ids"
    print(datetime.datetime.now(),f"{msg.upper()}\n")

    tasks(ano)

    # df = getdata("analytics_pulling_entrada",f"extract_receb_transform_{ano}")

    # print(df)

        # # gerar 3 bases em paralelo
        # list_futures = [executor.submit(task) for task in task1]
        # concurrent.futures.wait(list_futures)

        # for future in as_completed(futures):
        #     future.add_done_callback(custom_callback)
        #     results = future.result() 
            
        #     print("Waiting 60 seconds before the next batch...")
        #     time.sleep(60) 
        
