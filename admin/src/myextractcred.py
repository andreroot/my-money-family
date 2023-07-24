#
# conceito de pushout_credits: obter valor saldo inicial do credito e controlar pushout do credito
#

def my_credits_pushout_money(path,  df):
    import datetime as dt

    df["tipo_custo"] = None

    #
    # ETAPA INTERMEDIARIA QUE CLASSIFICA O TIPO DE PUSHOUT
    #

    from mytypedestinmoney import my_destin_pushout_csv
    df = my_destin_pushout_csv(df, f'{path}/csv/type.csv')

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
    import pandas as pd

    df['dt_extrato_bq'] = pd.to_datetime(df['dt_extrato_bq'],format='%d/%m/%Y')
    df['dt_base'] = pd.to_datetime(df['dt_base'],format='%d/%m/%Y')

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

def my_extract_excel_cred(path, file):
    import pandas as pd

    path_excel_file_cred = f'{path}/excel/{file}'

    df = pd.read_excel(path_excel_file_cred, sheet_name='Lançamentos', usecols = "A,B,D,E,F", skiprows=1) 
    df.columns = ["dt_extrato_bq", "descricao", "valor_ext","dt_base","valor_parc"]

    print(f'Etapa de conversão do type das colunas do dataframe gerada do excel:{file}','\n')

    df = myconverter(df)

    return df

# FUNÇÃO CHAMADA PARA APLICAR REGRAS PARA CADA TIPO D EMODA FINANCEIRA
def my_credits(df, path, file ):
    
    #verifica se dataframe tem dados
    from mygcptablefinfam import insert_df_credits

    if len(df):

        print(f"CREDITS:{file}", "\n")
        print(f"TOTAL DE LINHAS STOP: {len(df)}", "\n")

        insert_df_credits(my_credits_pushout_money(path, df), f'{path}/json/credits.json','devsamelo2.dev_domestico.credito_2023_excel')

    else:
        print("ERROR AO GERAR DATAFRAME", "\n")

