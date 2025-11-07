import os
import sys
import time
import datetime
import pandas as pd

from transform.mycalculateanalytics import calculate_analytics_sheets
from transform.myextracanalytics import extract_analytics_sheets

from sheets.generate_data import generate_parquet_process as generate

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import time

def tasks_old():

    tasks = [("extrato_2025", "extract_cust_transform"), ("extrato_2025", "extract_saldo_transform"), ("extrato_2025", "extract_receb_transform")]

    results = {}  # Ou use uma lista se preferir

    with ProcessPoolExecutor(max_workers=3)  as executor:
        #results = executor.map(tasks)
        futures = {executor.submit(generate, sheet_name, tab_name): tab_name for sheet_name, tab_name in tasks}
        
        for future in as_completed(futures):
            tab_name = futures[future]
            try:
                df = future.result()
                results[tab_name] = df  # Salva o DataFrame com a chave do tab_name
                print(f"Processado: {tab_name}, shape: {df.shape}")
            except Exception as e:
                print(f"Erro ao processar {tab_name}: {e}")

                                
        executor.shutdown(wait=True)


def tasks():

    tasks = [("stop_2025","extract_saldo_transform"), ("pulling_2025","extract_receb_transform"), ("pushout_2025","extract_cust_transform")]

    results = {}  # Ou use uma lista se preferir

    with ProcessPoolExecutor(max_workers=3)  as executor:
        #results = executor.map(tasks)
        futures = {executor.submit(generate_parquet_process, parquet, tab_name): tab_name for parquet, tab_name in tasks}
        
        for future in as_completed(futures):
            tab_name = futures[future]
            try:
                df = future.result()
                results[tab_name] = df  # Salva o DataFrame com a chave do tab_name
                print(f"Processado: {tab_name}, shape: {df.shape}")
            except Exception as e:
                print(f"Erro ao processar {tab_name}: {e}")

                                
        executor.shutdown(wait=True)

def results():

    # Agora você pode acessar cada DataFrame pelo nome da aba:
    dfc = results.get("extract_cust_transform")
    dfs = results.get("extract_saldo_transform")
    dfr = results.get("extract_receb_transform")

    with ThreadPoolExecutor(max_workers=2)  as executor:
        task1=[calculate_analytics_sheets(dfs, dfc, dfr), extract_analytics_sheets(dfc)]
        results = executor.map(tasks)
        

if __name__=='__main__':


    # dfc = generate(sheet_name="extrato_2025", tab_name="extract_cust_transform")
    # dfs = generate(sheet_name="extrato_2025", tab_name="extract_saldo_transform")
    # dfr = generate(sheet_name="extrato_2025", tab_name="extract_receb_transform")


    msg = f" Finance | 1/3 | Preparar dados para processamento, listagem de ids"
    print(datetime.datetime.now(),f"{msg.upper()}\n")

    tasks()

    results()


        # # gerar 3 bases em paralelo
        # list_futures = [executor.submit(task) for task in task1]
        # concurrent.futures.wait(list_futures)

        # for future in as_completed(futures):
        #     future.add_done_callback(custom_callback)
        #     results = future.result() 
            
        #     print("Waiting 60 seconds before the next batch...")
        #     time.sleep(60) 
        
