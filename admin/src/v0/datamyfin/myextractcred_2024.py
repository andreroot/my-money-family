#
# conceito de pushout_credits: obter valor saldo inicial do credito e controlar pushout do credito
#
    
#verifica se dataframe tem dados
from datamyfin.mytypedestinmoney_v1 import my_destin_pushout_csv

# insert
from insertmyfin.mygcptablefinfam import insert_df_credits


import re
import pandas as pd

def my_credits_pushout_money(path,  df):
    import datetime as dt

    df["tipo_custo"] = None


    #
    # ETAPA INTERMEDIARIA QUE CLASSIFICA O TIPO DE PUSHOUT
    #

    file_depara_csv="oficial-depara-custo"
    df = my_destin_pushout_csv(df, f'{path}\\v0\\data\\csv\\{file_depara_csv}.csv')

    df = df.rename(columns={'descricao': 'custo_credito'})
    df = df.rename(columns={'valor_ext': 'valor_credito'})
    df = df.rename(columns={'valor_parc': 'valor_credito_parc'})
    df = df.rename(columns={'dt_base': 'dt_mes_base'})
    df = df.rename(columns={'dt_extrato_bq': 'dt_credito'})
    df = df.rename(columns={'tipo_custo': 'tipo_custo_credito'})


    #INSERT DT PROCESS 
    now = dt.datetime.now()

    dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

    df['process_time'] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")
    df = df[["tipo_custo_credito", "custo_credito", "valor_credito", "valor_credito_parc", "dt_mes_base", "dt_credito","process_time"]]


    return df

# conversão de colunas do excel para string, date e float
def myconverter(df):

    df['dt_extrato_bq'] = pd.to_datetime(df['dt_extrato_bq'],format='%d/%m/%Y')
    #df['dt_base'] = pd.to_datetime(df['dt_base'],format='%d/%m/%Y')
    df['dt_base'] = pd.to_datetime(df['dt_base'],format='%Y/%m/%d')

    df["valor_ext"] = df["valor_ext"].astype(str)
    df["valor_ext"] = [x.replace(",", ".") for x in df["valor_ext"]]
    df["valor_ext"] = pd.to_numeric(df["valor_ext"].fillna(0), errors="coerce")
    df["valor_ext"] = df["valor_ext"].map("{:.2f}".format)
    df["valor_ext"] = df["valor_ext"].astype(float)

    df["valor_parc"] = df["valor_parc"].astype(str)
    df["valor_parc"] = [x.replace(",", ".") for x in df["valor_parc"]]
    df["valor_parc"] = pd.to_numeric(df["valor_parc"].fillna(0), errors="coerce")
    df["valor_parc"] = df["valor_parc"].map("{:.2f}".format)
    df["valor_parc"] = df["valor_parc"].astype(float)

    return df

#
# conceito de credito: acompanhamento do credito usado, ao fechamento do fluxo mensal devolvido ao terceiro
#

def my_extract_excel_cred(path, mes_ano, file):

    path_excel_file_cred = f'{path}\\v0\\data\\excel\\{file}'

    df = pd.read_excel(path_excel_file_cred, sheet_name='Lançamentos', usecols = "A,B,D", skiprows=1) 
    df.columns = ["dt_extrato_bq", "descricao", "valor_ext"]

    df["valor_cred_pontos"] = df.apply(lambda x: x['valor_ext'] if x['descricao'].find('Credito Prog De Pontos')>0 else 0 , axis=1)
    df["valor_ext"] = df.apply(lambda x: 0 if x['descricao'].find('Credito Prog De Pontos')>0 else x['valor_ext'] , axis=1)
    df["valor_parc"] = df.apply(lambda x: 0 if x['descricao'].find('Credito Prog De Pontos')>0 else x['valor_ext'] , axis=1)
    df['dt_base'] = mes_ano.replace("_", "/")+"/01"

    print(f'Etapa de conversão do type das colunas do dataframe gerada do excel:{file}','\n')

    df = myconverter(df)

    return df

# FUNÇÃO CHAMADA PARA APLICAR REGRAS PARA CADA TIPO D EMODA FINANCEIRA
def my_credits(df, path, file, ano_base ):


    if len(df):

        dt_base = re.findall(r'^credito_(\w+\_+\w+)',file)
        print(dt_base)

        print(f"CREDITS:{file}", "\n")
        print(f"TOTAL DE LINHAS CREDITS: {len(df)}", "\n")

        insert_df_credits(my_credits_pushout_money(path, df), f'{path}\\v0\\json\\credits.json',f'devsamelo2.dev_domestico.credito_{ano_base}_excel')

    else:
        print("ERROR AO GERAR DATAFRAME", "\n")

