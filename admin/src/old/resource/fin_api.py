import os
import pandas as pd
import pandas_gbq as pgbq 

# Construct a BigQuery client object.
from conectores.mygcpcredencial import my_credencial
from datamyfin.myextract_2024 import my_extract_excel_deb
from datamyfin.myextractcred_2024 import my_extract_excel_cred
from datamyfin.mygcptablefinfam import insert_df_pushout
from datamyfin.mytypedestinmoney_v1 import my_destin_pushout_csv

from google.cloud import bigquery


def consulta_excel(path, file_excel, tipo, mes_ano):
    #path = os.getcwd()
    if tipo=='debito':
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

    #CRIAR COLUNA QUE É USADA PARA DEPARA DE DEFINIÇÃO DE TIPO DE CUSTO
    df["descricao"] = df["custo"]

    file_depara_csv="oficial-depara-custo"
    df = my_destin_pushout_csv(df, f'{path_src}/csv/{file_depara_csv}.csv')

    df = df[["tipo_custo", "custo", "valor_custo", "dt_mes_base", "dt_custo","process_time"]]

    return df

def analytics(df, data_base, MES):

    total_geral = df['valor_custo'][(pd.to_datetime(df['dt_mes_base'])==data_base)].sum()
    total_geral_fixo_al = df.loc[(df['classificacao_custo']=='fixo') & (df['area_custo']=='alto') & (pd.to_datetime(df['dt_mes_base'])==data_base)& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
    total_geral_fixo_md = df.loc[(df['classificacao_custo']=='fixo') & (df['area_custo']=='medio') &  (pd.to_datetime(df['dt_mes_base'])==data_base)& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
    total_geral_fixo_bx = df.loc[(df['classificacao_custo']=='fixo') & (df['area_custo']=='medio_n1') &  (pd.to_datetime(df['dt_mes_base'])==data_base)& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
    total_geral_variado = df.loc[(df['classificacao_custo']=='variado') & (pd.to_datetime(df['dt_mes_base'])==data_base)]['valor_custo'].sum()

    msg=f"{MES} | custo geral: {total_geral} | custo fixo(alto): {total_geral_fixo_al} | custo fixo(medio): {total_geral_fixo_md} | custo fixo(baixo): {total_geral_fixo_bx}| custo variado: {total_geral_variado}"

    return msg


def analytcis_mes_custo(df):

    import datetime
    from datetime import date, timedelta
    from calendar import monthrange

    mes=[]
    #list_data=['2024-01-01','2024-01-01','2024-01-01','2024-04','2024-05', '2024-06']

    for m in range(1,13):
        print(m)
        dt_mes_base = f'2024-{m}-01'

        dt_ini=datetime.datetime.strptime(dt_mes_base, '%Y-%m-%d')+timedelta(days=0)
        print(dt_ini)
        dt_fin=dt_ini.replace(day=monthrange(dt_ini.year, dt_ini.month)[1])+timedelta(days=0)
        print(dt_fin)

        t = df.loc[(pd.to_datetime(df['dt_mes_base'])==dt_ini.strftime('%Y-%m-%d')) \
                & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
        
        f = df.loc[(df['classificacao_custo']=='fixo') & (pd.to_datetime(df['dt_mes_base'])==dt_ini.strftime('%Y-%m-%d')) \
                & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
        
        v = df.loc[(df['classificacao_custo']=='variado') & (pd.to_datetime(df['dt_mes_base'])==dt_ini.strftime('%Y-%m-%d')) \
                    & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))]['valor_custo'].sum()
        

        # dt_mes_base_ant=(dt_ini+timedelta(days=-1)).replace(day=1)
        # dt_ini_ant=dt_mes_base_ant+timedelta(days=0)
        # print(dt_ini_ant)
        # dt_fin_ant=dt_ini_ant.replace(day=monthrange(dt_ini_ant.year, dt_ini_ant.month)[1])+timedelta(days=0)
        # print(dt_fin_ant)

        # v_mes_ant = df.loc[(df['classificacao_custo']=='variado') & (pd.to_datetime(df['dt_mes_base'])==dt_ini_ant.strftime('%Y-%m-%d'))\
        #                 & ((pd.to_datetime(df['dt_custo'])>=dt_ini_ant)&(pd.to_datetime(df['dt_custo'])<=dt_fin_ant))]['valor_custo'].sum()

        # if v_mes_ant>0:
        #     perc = (v*100)/v_mes_ant
        # else:
        #     perc =0
        if t>0:
            perc_f=round(float((f/t)*100),2)
            perc_v=round(float((v/t)*100),2)
        else:
            perc_f=0
            perc_v=0

        ar=[dt_ini.strftime('%Y-%m'),dt_ini,t,f, perc_f,v, perc_v, dt_fin]
        mes.append(ar)

    return mes


def analytcis_mes_custo_periodo(df, dias_1, dias_2):
 
    import datetime
    from datetime import date, timedelta
    from calendar import monthrange

    mes=[]
    #list_data=['2024-01-01','2024-01-01','2024-01-01','2024-04','2024-05', '2024-06']

    for m in range(1,13):
        #print(m)
        dt_mes_base = f'2024-{m}-01'

        dt_ini=datetime.datetime.strptime(dt_mes_base, '%Y-%m-%d')+timedelta(days=dias_1)
        #print(dt_ini)
        dt_fin=dt_ini.replace(day=monthrange(dt_ini.year, dt_ini.month)[1])+timedelta(days=dias_2)
        #print(dt_fin)

        t = df.loc[((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))]['valor_custo'].sum()
        
        f = df.loc[(df['classificacao_custo']=='fixo')\
                & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
        
        v = df.loc[(df['classificacao_custo']=='variado')\
                    & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))]['valor_custo'].sum()
        


        # dt_mes_base_ant=(dt_ini+timedelta(days=-1)).replace(day=1)
        # dt_ini_ant=dt_mes_base_ant+timedelta(days=dias_1)
        # print(dt_ini_ant)
        # dt_fin_ant=dt_ini_ant.replace(day=monthrange(dt_ini_ant.year, dt_ini_ant.month)[1])+timedelta(days=dias_2)
        # print(dt_fin_ant)

        # v_mes_ant = df.loc[(df['classificacao_custo']=='variado') & (pd.to_datetime(df['dt_mes_base'])==dt_ini_ant.strftime('%Y-%m-%d'))\
        #                 & ((pd.to_datetime(df['dt_custo'])>=dt_ini_ant)&(pd.to_datetime(df['dt_custo'])<=dt_fin_ant))]['valor_custo'].sum()

        # if v_mes_ant>0:
        #     perc = (v/v_mes_ant)*100
        # else:
        #     perc =0
        if t>0:
            perc_f=float((f/t)*100)
            perc_v=float((v/t)*100)
        else:
            perc_f=0
            perc_v=0

        ar=[dt_ini.strftime('%Y-%m'),dt_ini,t,f, perc_f,v, perc_v, dt_fin]
        mes.append(ar)

        # ar=[dt_ini.strftime('%Y-%m'),dt_ini,t,f,v,dt_fin, dt_mes_base_ant, v_mes_ant, perc]
        # mes.append(ar)

    return mes


def analytcis_mes_custo_variavel(df, list_tp):

 
    import datetime
    from datetime import date, timedelta
    from calendar import monthrange

    mes=[]
    #list_data=['2024-01-01','2024-01-01','2024-01-01','2024-04','2024-05', '2024-06']

    for m in range(1,13):
        #print(m)
        dt_mes_base = f'2024-{m}-01'

        dt_ini=datetime.datetime.strptime(dt_mes_base, '%Y-%m-%d')+timedelta(days=0)
        #print(dt_ini)
        dt_fin=dt_ini.replace(day=monthrange(dt_ini.year, dt_ini.month)[1])+timedelta(days=0)
        #print(dt_fin)

        for str in list_tp:

            #print(str)
            v = df.loc[(pd.to_datetime(df['dt_mes_base'])==dt_ini.strftime('%Y-%m-%d'))& (df['tipo_custo_alt']==str)  \
                    & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))]['valor_custo'].sum()
        

            dt_mes_base_ant=(dt_ini+timedelta(days=-1)).replace(day=1)
            dt_ini_ant=dt_mes_base_ant+timedelta(days=0)
            #print(dt_ini_ant)
            dt_fin_ant=dt_ini_ant.replace(day=monthrange(dt_ini_ant.year, dt_ini_ant.month)[1])+timedelta(days=0)
            #print(dt_fin_ant)

            v_mes_ant = df.loc[ (pd.to_datetime(df['dt_mes_base'])==dt_ini_ant.strftime('%Y-%m-%d'))& (df['tipo_custo_alt']==str)\
                            & ((pd.to_datetime(df['dt_custo'])>=dt_ini_ant)&(pd.to_datetime(df['dt_custo'])<=dt_fin_ant))]['valor_custo'].sum()

            if v_mes_ant>0:
                perc = (v/v_mes_ant)*100
            else:
                perc =0

            ar=[dt_ini.strftime('%Y-%m-%d'),str,dt_ini,v,dt_fin, dt_mes_base_ant, v_mes_ant, perc]
            mes.append(ar)

         
    return mes