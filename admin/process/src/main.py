import os
import sys
import time
import datetime
import pandas as pd

from transform.mycalculateanalytics import calculate_analytics_sheets
from transform.myextracanalytics import extract_analytics_sheets

from sheets.generate_data import generate_process as generate

from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import time

class Main:

    def __init__(self):
        pass

    def insert_data_fake(self, df, list_cust):
        
        df_list_merge = []

        for fkmonth in range(7, 13):

            mes = f'2025-{fkmonth:02d}-01'

            df_fake = fake(
                mes,
                f'2025-{fkmonth:02d}-05',
                list_cust['tipo_custo'],
                list_cust['custo'],
                list_cust['valor']
            )

            #print(df_fake.head(5))

            df_list_merge.append(df_fake)

        df_merge = pd.DataFrame()

        # Faz o merge de todos os DataFrames gerados
        df_merge = pd.concat([df] + df_list_merge, ignore_index=True)

        df_merge['valor_custo'] = df_merge.apply(lambda x: 1000 if (x['tipo_custo_alt']=='emprestimo' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-07-01') 
                                                        else( 1400  if (x['tipo_custo_alt']=='emprestimo' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-08-01') 
                                                            else( 1400  if (x['tipo_custo_alt']=='emprestimo' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-10-01')
                                                else x['valor_custo'])), axis=1) 

        df_merge['valor_custo'] = df_merge.apply(lambda x: 130 if (x['tipo_custo_alt']=='escola_futebol' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-11-01') 
                                                        else( 130  if (x['tipo_custo_alt']=='escola_futebol' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-10-01') 
                                                            else( 130  if (x['tipo_custo_alt']=='escola_futebol' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-12-01')
                                                else x['valor_custo'])), axis=1) 


        df_merge['valor_custo'] = df_merge.apply(lambda x: 800 if (x['tipo_custo_alt']=='cartao' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-07-01') 
                                                        else( 800  if (x['tipo_custo_alt']=='cartao' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-08-01') 
                                                            else( 800  if (x['tipo_custo_alt']=='cartao' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-09-01')
                                                                 else( 800  if (x['tipo_custo_alt']=='cartao' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-10-01')
                                                                      else( 400  if (x['tipo_custo_alt']=='cartao' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-11-01')
                                                                           else( 400  if (x['tipo_custo_alt']=='cartao' and datetime.datetime.strftime(x['dt_mes_base'],"%Y-%m-%d")=='2025-12-01')
                                                else x['valor_custo']))))), axis=1)         
        #print(df_merge[df_merge['tipo_custo_alt']=='emprestimo'].head(5))
        
        return df_merge


if __name__=='__main__':


    # dfc = generate(sheet_name="extrato_2025", tab_name="extract_cust_transform")
    # dfs = generate(sheet_name="extrato_2025", tab_name="extract_saldo_transform")
    # dfr = generate(sheet_name="extrato_2025", tab_name="extract_receb_transform")


    msg = f" Finance | 1/3 | Preparar dados para processamento, listagem de ids"
    print(datetime.datetime.now(),f"{msg.upper()}\n")

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

    # Agora você pode acessar cada DataFrame pelo nome da aba:
    dfc = results.get("extract_cust_transform")
    dfs = results.get("extract_saldo_transform")
    dfr = results.get("extract_receb_transform")

    with ThreadPoolExecutor(max_workers=2)  as executor:
        task1=[calculate_analytics_sheets(dfs, dfc, dfr), extract_analytics_sheets(dfc)]
        results = executor.map(tasks)
        
        # # gerar 3 bases em paralelo
        # list_futures = [executor.submit(task) for task in task1]
        # concurrent.futures.wait(list_futures)

        # for future in as_completed(futures):
        #     future.add_done_callback(custom_callback)
        #     results = future.result() 
            
        #     print("Waiting 60 seconds before the next batch...")
        #     time.sleep(60) 
        
