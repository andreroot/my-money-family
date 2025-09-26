import os
import sys
import time
import datetime
import pandas as pd

from transform.myanalytics import calculate_analytics

from sheets.generate_data import generate_process as generate
from sheets.generate_data import analytics_sheets

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


    dfc = generate(sheet_name="extrato_2025", tab_name="extract_cust_transform")
    dfs = generate(sheet_name="extrato_2025", tab_name="extract_saldo_transform")
    dfr = generate(sheet_name="extrato_2025", tab_name="extract_receb_transform")

    # plano financeiro - previsão
    dfp = calculate_analytics(dfs, dfc, dfr)

    analytics_sheets(dfp)

    print(dfp.head())

