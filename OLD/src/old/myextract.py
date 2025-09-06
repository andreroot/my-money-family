#
# conceito de pull: puxar algo, são os recebidos puxados para o caixa financeiro, são a origem do fluxo
#

def my_pulling_money(df):
    import datetime as dt

    print('Inicio da etapa origem do dinheiro: puxar algo para dentro, origem do fluxo', "\n")

    df = df.rename(columns={'valor_ext': 'valor_recebido'})
    df = df.rename(columns={'dt_base': 'dt_mes_base'})
    df = df.rename(columns={'dt_extrato_bq': 'dt_recebido'})
    
    now = dt.datetime.now()
    dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))
    df['process_time'] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")
    df = df[[ "descricao", "valor_recebido", "dt_mes_base", "dt_recebido", "process_time"]]

    return df

#
# conceito de push: empurrar algo, são os gastos empurrados para fora do caixa financeiro para algum destino, são o destino final do fluxo
#
def my_pushout_money(path, df):
    import datetime as dt

    print('Inicio da etapa destino do dinheiro: empurra pra fora', "\n")

    df['tipo_custo']=None

    #
    # ETAPA INTERMEDIARIA QUE CLASSIFICA O TIPO DE PUSHOUT
    #

    #
    from mytypedestinmoney_v1 import my_destin_pushout_csv
    file_depara_csv="oficial-depara-custo"
    df = my_destin_pushout_csv(df, f'{path}/csv/{file_depara_csv}.csv')

    df = df.rename(columns={'descricao': 'custo'})
    df = df.rename(columns={'valor_ext': 'valor_custo'})
    df = df.rename(columns={'dt_base': 'dt_mes_base'})
    df = df.rename(columns={'dt_extrato_bq': 'dt_custo'})

    #INSERT DT PROCESS           
    now = dt.datetime.now()
    dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))
    df['process_time'] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")
    df = df[["tipo_custo", "custo", "valor_custo", "dt_mes_base", "dt_custo","process_time"]]

    return df

def my_stop_money(df):
    import datetime as dt

    print('Inicio da etapa saldo do mes: fechamento parcial ou fim do mes', "\n")

    df = df.rename(columns={'valor_saldo': 'saldo'})
    df = df.rename(columns={'dt_base': 'dt_mes_base'})
    df = df.rename(columns={'dt_extrato_bq': 'dt_recebido'})
    #print("-> inserir data_process", "\n")
    
    now = dt.datetime.now()
    dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))
    df['process_time'] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")
    df = df[[ "descricao", "saldo", "dt_mes_base", "dt_recebido", "process_time"]]

    return df



# conversão de colunas do excel para string, date e float
def myconverter(df):
    import pandas as pd
    import datetime as dt

    df['dt_extrato_bq'] = pd.to_datetime(df['dt_extrato_bq'],format='%d/%m/%Y')
    
    if len(df)==1:
        df['ano'] = pd.to_datetime(df['dt_extrato_bq'].iloc[0]).strftime("%Y")
        df['mes'] = pd.to_datetime(df['dt_extrato_bq'].iloc[0]).strftime("%m")
        df_dt_base = dt.datetime(year=int(df['ano'].iloc[0]),month=int(df['mes'].iloc[0]), day=1)
    else:
        df['ano'] = pd.to_datetime(df['dt_extrato_bq'].iloc[1]).strftime("%Y")
        df['mes'] = pd.to_datetime(df['dt_extrato_bq'].iloc[1]).strftime("%m")
        df_dt_base = dt.datetime(year=int(df['ano'].iloc[1]),month=int(df['mes'].iloc[1]), day=1)            

    #REGRA 1: GERADO DOIS CAMPOS NOVOS DT BASE PARA DTEERMINAR A DATA BASE MES E ANO DO CONTROLE FINANCEIRO
    df['dt_base'] = df_dt_base 
    #REGRA 2: GERADO CAMPO ORIGINAL DO EXTRATO DO DEBITO PARA DETERMINAR DATAFRAME PUSHOUT(VALOR NEGATIVO / SAIDA) E PULLING(VALOR POSITIVO / ENTRADA)
    df["valor_alt"] = df["valor_ext"]

    df["valor_ext"] = df["valor_ext"].astype(str)
    df["valor_ext"] = [x.replace(",", ".") for x in df["valor_ext"]]
    df["valor_ext"] = pd.to_numeric(df["valor_ext"].fillna(0), errors="coerce")
    df["valor_ext"] = df["valor_ext"].map("{:.2f}".format)
    df["valor_ext"] = df["valor_ext"].astype(float)
    df["valor_ext"] = df["valor_ext"].abs()

    #print(df, "\n")

    df["valor_saldo"] = df["valor_saldo"].astype(str)
    df["valor_saldo"] = [x.replace(",", ".") for x in df["valor_saldo"]]
    df["valor_saldo"] = pd.to_numeric(df["valor_saldo"].fillna(0), errors="coerce")
    df["valor_saldo"] = df["valor_saldo"].map("{:.2f}".format)
    df["valor_saldo"] = df["valor_saldo"].astype(float)

    

    return df

# FUNÇÃO CHAMADA PARA PREPARAR O DATAFRAME PARA PROCESSO DEFINIÇÃO DO TIPO DE MODA FINANCEIRA
def my_extract_excel_deb(path, file):
    import pandas as pd    
    path_excel_file_deb = f'{path}/excel/{file}'

    df = pd.read_excel(path_excel_file_deb, sheet_name='Lançamentos', usecols = "A,B,D,E", skiprows=8) 
    df.columns = ["dt_extrato_bq", "descricao", "valor_ext", "valor_saldo" ]

    print(f'Etapa de conversão do type das colunas do dataframe gerada do excel:{file}','\n')

    df = myconverter(df)

    return df

# FUNÇÃO CHAMADA PARA APLICAR REGRAS PARA CADA TIPO D EMODA FINANCEIRA
def my_type_modal_finance(df, path, file, tipo_doc_extract ):
    
    #verifica se dataframe tem dados

    if len(df):
        #
        # conceito de pull: puxar algo, são os recebidos puxados para o caixa financeiro, são a origem do fluxo
        #
        if tipo_doc_extract=='pulling':
            from mygcptablefinfam import insert_df_pulling

            print(f"ENTRADA / RECEBIDOS / PULLING:{file}", "\n")
            df = df.loc[df['valor_alt'] >= 0].copy()
            df = df[df['descricao'].str.contains(r'SALDO[^\\b]+\w')==False].copy()
            df = df[df['descricao'].str.contains(r'RES APLIC[^\\b]+\w')==False].copy()
            #df = df[df['descricao'].str.contains(r'REND PAGO[^\\b]+\w')==False].copy()

            print(f"TOTAL DE LINHAS PULLING: {len(df)}", "\n")

            insert_df_pulling(my_pulling_money(df), f'{path}/json/pulling.json','devsamelo2.dev_domestico.recebido_2023_excel')

        #
        # conceito de push: empurrar algo, são os gastos empurrados para fora do caixa financeiro para algum destino, são o destino final do fluxo
        #
        elif tipo_doc_extract=='pushout':
            from mygcptablefinfam import insert_df_pushout

            print(f"SAIDA / CUSTO / PUSHOUT:{file}", "\n")
            df = df.loc[df['valor_alt'] < 0].copy()
            df = df.loc[df['descricao'].str.contains(r'SALDO[^\\b]+\w|APL[^\\b]APLIC[^\\b]AUT[^\\b]MAIS|RES[^\\b]APLIC[^\\b]AUT[^\\b]MAIS|REND[^\\b]PAGO[^\\b]APLIC[^\\b]AUT[^\\b]MAIS')==False].copy()
            # df = df.loc[df['descricao'].str.contains(r'APL[^\\b]APLIC[^\\b]AUT[^\\b]MAIS')==False].copy()
            # df = df.loc[df['descricao'].str.contains(r'RES[^\\b]APLIC[^\\b]AUT[^\\b]MAIS')==False].copy()         
            
            print(f"TOTAL DE LINHAS PUSHOUT: {len(df)}", "\n")

            insert_df_pushout(my_pushout_money(path, df), f'{path}/json/pushout.json','devsamelo2.dev_domestico.custo_2023_excel')

            
        #
        # conceito de stop: parado em algum lugar, saldo do caixa financeiro para inicio de um novo ciclo do fluxo
        #
        elif tipo_doc_extract=='stop':
            from mygcptablefinfam import insert_df_stop

            print(f"SALDO / NEGATICO OU POSITIVO / STOP:{file}", "\n")
            df = df.loc[(df['descricao'].str.contains(r'SALDO[^\\b]+\w|SDO[^\\b]+CTA\/+APL[^\\b]+\w'))].copy()

            print(f"TOTAL DE LINHAS STOP: {len(df)}", "\n")

            insert_df_stop(my_stop_money(df), f'{path}/json/stop.json','devsamelo2.dev_domestico.saldo_2023_excel') 
    else:
        print("ERROR AO GERAR DATAFRAME", "\n")

