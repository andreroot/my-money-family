import os
import pandas as pd
import pandas_gbq as pgbq 

# Construct a BigQuery client object.
from mygcpcredencial import my_credencial
from google.cloud import bigquery


def consulta_excel(path, file_excel, tipo, mes_ano):
    #path = os.getcwd()
    if tipo=='debito':
        from myextract_2024 import my_extract_excel_deb
        my_fin = my_extract_excel_deb

        print(f'{path}/excel/{file_excel}.xls')

        if os.path.exists(f'{path}/excel/{file_excel}.xls'):
            print('\n')
            df = my_fin(path, f'{file_excel}.xls')
            
            print(f"TAMANHO TOTAL DF: {len(df)}")
        else:
            df = pd.DataFrame()

        return df
    elif tipo=='credito':

        from myextractcred_2024 import my_extract_excel_cred
        my_fin = my_extract_excel_cred

        print('\n')
        df = my_fin(path, mes_ano, f'credito_{mes_ano}.xls')

    
        print(f"TAMANHO TOTAL DF: {len(df)}")

        print('\n')

        return df




def consulta_csv_depara(path, file_csv):

    print(f'{path}/csv/{file_csv}.csv')

    if os.path.exists(f'{path}/csv/{file_csv}.csv'):
        print('\n')
        df = pd.read_csv(f'{path}/csv/{file_csv}.csv', sep=';', usecols=['de_para','valor'], encoding='latin-1')
        
        print(f"TAMANHO TOTAL DF: {len(df)}")
    else:
        df = pd.DataFrame()

    return df

def consulta_gcp(query):

    credentials = my_credencial()
    # Construct a BigQuery client object.
    #client = bigquery.Client(credentials=credentials, project='devsamelo2')

    # strsql = geraScriptSql(f'{path}/query_cust/QUERY_ANALISE_CUSTO_TABELAS.sql',None,None) #,value[1])
    #GERAR DATAFRAME
    df = pgbq.read_gbq(query, project_id='devsamelo2' , credentials=credentials, progress_bar_type=None)

    return df

def manutencao_dados(df, path_src, ano, periodo):
    from mygcpjobfinfam import my_execute_job
    my_job = my_execute_job

    from mygcptablefinfam import insert_df_pushout

    print(f"SAIDA / CUSTO / PUSHOUT:", "\n")
    print(f'Primeira etapa gerar dados controle da conta debito: Limpeza de dados da tabela: custo_2023_excel!!!','\n')

    if ano=='2023':
        query = f'DELETE FROM dev_domestico.custo_2023_excel where dt_mes_base = "{periodo}"'
        my_job(query)

        insert_df_pushout(df, f'{path_src}/json/pushout.json','devsamelo2.dev_domestico.custo_2023_excel')
    else:
        query = f'DELETE FROM dev_domestico.custo_2024_excel where dt_mes_base = "{periodo}"'
        my_job(query)

        insert_df_pushout(df, f'{path_src}/json/pushout.json','devsamelo2.dev_domestico.custo_2024_excel')

    

def aplicar_regra_tipo_custo(df, path_src):
    from mytypedestinmoney_v1 import my_destin_pushout_csv

    #CRIAR COLUNA QUE É USADA PARA DEPARA DE DEFINIÇÃO DE TIPO DE CUSTO
    df["descricao"] = df["custo"]

    file_depara_csv="oficial-depara-custo"
    df = my_destin_pushout_csv(df, f'{path_src}/csv/{file_depara_csv}.csv')

    df = df[["tipo_custo", "custo", "valor_custo", "dt_mes_base", "dt_custo","process_time"]]

    return df