
from resource.fin_api import consulta_gcp

import pandas as pd   
import pandas_gbq as pgbq 
import datetime as dt
import re
from datetime import timedelta, date


def criar_calendario(df):
    df['week_br'] = pd.to_datetime(df["dt_custo"]).dt.day_name()
    df['week_br'] = df.apply(lambda x: "segunda-feira" if x["week_br"]=="Monday" else 
                                ("terça-feira" if x["week_br"]=="Tuesday" else 
                                ("quarta-feira" if x["week_br"]=="Wednesday" else 
                                ("quinta-feira" if x["week_br"]=="Thursday" else 
                                ("sexta-feira" if x["week_br"]=="Friday" else 
                                ("sabado" if x["week_br"]=="Saturday" else "domingo"))))), axis=1)

    return df
# Monday (segunda-feira)
# Tuesday (terça-feira)
# Wednesday (quarta-feira)
# Thursday (quinta-feira)
# Friday (sexta-feira)
# Saturday (sábado)
# Sunday (domingo)

#df[df['tipo_custo'].str.contains(r'feira-\w+|feira')==True].groupby('dt_mes_base')['valor_custo'].sum()
#df[["dt_mes_base","tipo_custo","valor_custo"]][((pd.to_datetime(df['dt_custo'])>='2024-01-16'))&((pd.to_datetime(df['dt_custo'])<='2024-01-31'))&(df["tipo_custo"].isin(["emprestimo","financiamento"]))].groupby(['dt_mes_base','tipo_custo']).agg(['sum'])


def construcao_tipo_custo(df):

    df["tipo_custo_alt"]=df.apply(lambda x: ''.join(re.findall('^\w+',x["tipo_custo"])), axis=1)
  
    lista_tp=['igreja','futebol','claro','banco','boleto','escola_futebol','luz','financiamento','gas','net','ajuda','emprestimo','ipva','sabesp','saque','porto_seguro','cartao','dora','Taxa_banco','carro','poupanca','transporte_pub','recargapay','juros','cea']

    list_tp_alto=['financiamento','emprestimo','boleto','cartao','juros','banco','Taxa_banco','saque']
    list_tp_medio=['luz','gas','net','sabesp','claro']
    list_tp_medio_n1=['alimentacao','feira','mercado','padaria','transporte_pub','recargapay','cea','ifood','cabelereiro','farmacia','roupa','ajuda','pernambucanas','dora']
    list_tp_invest=['picpay','inter','nubank','pagbank']# remover poupanca
    list_tp_imprevisto=['pedreiro','consertotv','refil','flores','cartorio','shopmetro','lojaesportes','utensilios','app']
    list_tp_lazer=['brinquedo-praia','lazer','casamento','cinema','brinquedo','passeio','futebol','escola_futebol','churrasco-pessoal','futebol_amigos']
    list_tp_comercio=['pessoa','loja','cosmeticos','comercio','BAZAR','kalung','americanas']
    list_tp_carro=['porto_seguro','multa','uber','estacionamento','carro','pedagio','ipva','gasolina','mecanico','locadora','material_construcao_banheiro','celular','eletrolux']
    list_tp_igreja=['igreja']


    df["classificacao_custo"] = df.apply(lambda x: 'fixo' if [tp for tp in lista_tp if tp==x["tipo_custo_alt"]] else 'variado', axis=1)

    df["area_custo"] = df.apply(lambda x: 'alto' if [tp for tp in list_tp_alto if tp==x["tipo_custo_alt"] ] else 
                                ('medio' if [tp for tp in list_tp_medio if tp==x["tipo_custo_alt"]] else 
                                ('medio_n1' if [tp for tp in list_tp_medio_n1 if tp==x["tipo_custo_alt"]] else 
                                    ('invet' if [tp for tp in list_tp_invest if tp==x["tipo_custo_alt"]] else
                                        ('imprevisto' if [tp for tp in list_tp_imprevisto if tp==x["tipo_custo_alt"]] else 
                                        ('lazer' if [tp for tp in list_tp_lazer if tp==x["tipo_custo_alt"]] else 
                                            ('comercio' if [tp for tp in list_tp_comercio if tp==x["tipo_custo_alt"]] else
                                            ('carro' if [tp for tp in list_tp_carro if tp==x["tipo_custo_alt"]]  else 
                                                ('igreja' if [tp for tp in list_tp_igreja if tp==x["tipo_custo_alt"]]  else 
                                                ('poupanca' if x["tipo_custo_alt"]=='poupanca' else 'baixo'
                                    ))))))))), axis=1)

    return df


def fake( dt, tipo_custo, custo, valor_custo):
    #dfc.columns

    import pandas as pd   
    import datetime
    from datetime import timedelta, date

    # tipo_custo=['ajuda','celular','net','financiamento','emprestimo','cea']
    # custo=['DA  CLARO MOVEL 66459258','DA  ELETROPAULO 11701161','DA  NET SERVICOS 5680788','FINANC IMOBILIARIO','PIX ELIZABTEH','PIX QRS CEA PAY14/05']
    # valor_custo=[149.00, 127.42, 236.51, 2773.27, 1400, 364.28]

        # 21/10/2024	DA  NET SERVICOS 5680788		-236,51
        # 22/10/2024	DA  COMGAS 51448254		        -305,70
        # 23/10/2024	FINANC IMOBILIARIO   031		-2.773,27

    #datetime.datetime.strptime(datetime.datetime.now(), '%Y-%m-%d')
    #datetime.datetime.strptime(dt, '%Y-%m-%d')+timedelta(days=dias_1)
    dt_time_atual=datetime.datetime.now()
    print(datetime.datetime.strftime(dt_time_atual, '%Y-%m-%d %T.%f'))

    df_fake=pd.DataFrame()

    df_fake = pd.DataFrame({
        'tipo_custo':tipo_custo,
        'custo': custo,
        'valor_custo': valor_custo,
        'dt_mes_base':[pd.Timestamp(datetime.datetime.strptime(dt, '%Y-%m-%d').date()).date() for x in range(1,len(tipo_custo)+1)],
        'dt_custo':[pd.Timestamp((datetime.datetime.strptime(dt, '%Y-%m-%d')+timedelta(days=x)).date()).date() for x in range(1,len(tipo_custo)+1)],
        'process_time':[pd.Timestamp(datetime.datetime.strftime(dt_time_atual, '%Y-%m-%d %T.%f')) for x in range(1,len(tipo_custo)+1)]
        })


    return df_fake


def process(query, flag_fake=False):

    df = consulta_gcp(query)

    df=criar_calendario(df)

    # validacao 

    nulos=df[(df["tipo_custo"].isnull())][['custo','valor_custo','dt_custo']].drop_duplicates().values.tolist()

    if len(nulos):
        print('verificar nulos:',nulos)
        
        df=df[~(df["tipo_custo"].isnull())].copy()

    df=construcao_tipo_custo(df)

    df_tp=df[["tipo_custo_alt"]].drop_duplicates()
    #df.groupby(['tipo_custo']).agg({'tipo_custo':'count'}).copy()
    contagem_valores = df["tipo_custo_alt"].value_counts()
    list_tp=contagem_valores.index.tolist()
    # df_fake = df.loc[df['tipo_custo'].isin(list_tp)].copy()
    df_tp[~df_tp['tipo_custo_alt'].isin(['financiamento','emprestimo','boleto','cartao','juros','banco','Taxa_banco','saque',
                                        '#','luz','gas','net','sabesp','claro',
                                        '#','alimentacao','feira','mercado','padaria','transporte_pub','recargapay','cea','ifood','cabelereiro','cabeleireiro','farmacia','roupa','igreja','ajuda','pernambucanas','dora',
                                        '#','poupanca','picpay','inter','nubank','pagbank','nomad',
                                        '#','pedreiro','consertotv','refil','flores','cartorio','shopmetro','lojaesportes','utensilios','app',
                                        '#','brinquedo-praia','lazer','casamento','cinema','brinquedo','passeio','futebol','escola_futebol','churrasco-pessoal',
                                        '#','pessoa','loja','cosmeticos','comercio','BAZAR','kalung','americanas','presente','pessoal','rafael',
                                        '#','porto_seguro','multa','uber','estacionamento','carro','pedagio','ipva','gasolina','mecanico','locadora','material_construcao_banheiro','celular','eletrolux'])]

    # tratar gasto pessoal
    df['tipo_custo_alt'] = df.apply(lambda x: 'churrasco-pessoal' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
                                                                    x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-09-01' and
                                                                    x["dt_custo"].strftime('%Y-%m-%d')=='2024-09-02' and
                                                                    x["valor_custo"]==468.25  else x['tipo_custo_alt'], axis=1)

    df['tipo_custo'] = df.apply(lambda x: 'churrasco-pessoal' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
                                                                    x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-09-01' and
                                                                    x["dt_custo"].strftime('%Y-%m-%d')=='2024-09-02' and
                                                                    x["valor_custo"]==468.25  else x['tipo_custo'], axis=1)


    return df

def analytics_percent(query):

    #query = f"SELECT * FROM dev_domestico.custo_2024_excel where dt_mes_base  "

    df = process(query, flag_fake=False)

    list_tp=df.loc[(df['classificacao_custo']=='fixo')&(df['tipo_custo_alt']!='poupanca')][["tipo_custo_alt"]].drop_duplicates().values.tolist()
    arr_tp_ext=[]
    for idx, rw in enumerate(list_tp):
        #print(rw)
        valor_sum_anual = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_med_anual =  float(valor_sum_anual[0]/6)
        valor_jan   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-01-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_fev   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mar   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_abr   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mai   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jun   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jul   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_ago   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_set   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_out   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_nov   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_dez   = df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)

        # df_anual_jan = df[["tipo_custo_alt","valor_custo"]][ (df['classificacao_custo']=='fixo')&(pd.to_datetime(df['dt_mes_base'])=='2024-01-01')].groupby(['tipo_custo_alt'])\
        #     .agg(jan=('valor_custo','sum')) #.sort_values("valor_custo", ascending=False)

        # df_anual_fev = df[["tipo_custo_alt","valor_custo"]][ (df['classificacao_custo']=='fixo')&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].groupby(['tipo_custo_alt'])\
        #     .agg(jan=('valor_custo','sum')) #.sort_values("valor_custo", ascending=False)
        
        # df_tp=pd.merge(df_anual, df_anual_jan, df_anual_fev, how="left", on=["tipo_custo_alt"])

        ar=[rw[0],valor_sum_anual[0],valor_med_anual,valor_jan[0],valor_fev[0],valor_mar[0],valor_abr[0],valor_mai[0],valor_jun[0],valor_jul[0],valor_ago[0],valor_set[0],valor_out[0],valor_nov[0], valor_dez[0]]
        arr_tp_ext.append(ar)

    df_tp = pd.DataFrame(arr_tp_ext, columns=['tipo','vlr_sum','vlr_med','vlr_jan','vlr_fev','vlr_mar','vlr_abr','vlr_mai','vlr_jun','vlr_jul','vlr_ago','vlr_set','vlr_out','vlr_nov','vlr_dez'])

    # df[ (df['tipo_custo_alt'])=='cabelereiro']
    #& (pd.to_datetime(df['dt_custo'])=='2024-09-02') & (df['valor_custo']==468.25) &(df['custo']=='ELECTRONATACADAO 06')]
    # df['tipo_custo'] = df.apply(lambda x: 'cabeleireiro' if x["tipo_custo"].find("cabelereiro")>=0 else x['tipo_custo'],axis=1 )

    return df_tp

    # if flag_fake:
    #     tipo_custo=['ajuda','banco','celular','net','emprestimo','financiamento', 'cartao']
    #     custo=['DEB AUTOR UNICEF','SEGURO CARTAO','DA  CLARO MOVEL 66459258','DA  NET SERVICOS 5680788','PIX ELIZABTEH','FINANC IMOBILIARIO   029','CART ITAU']
    #     valor_custo=[85.00, 8.18, 149.90, 294.40, 1400, 2781.33, 883 ]
    #     dt='2024-08-01'
    #     df_fake = fake(None, dt,tipo_custo, custo, valor_custo)


def analytics_variado_percent(query):

    df = process(query, flag_fake=False)

    list_tp=df.loc[df['classificacao_custo']=='variado'][["tipo_custo_alt"]].drop_duplicates().values.tolist()
    arr_tp_ext=[]
    for idx, rw in enumerate(list_tp):
        #print(rw)
        valor_sum_anual = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_med_anual =  float(valor_sum_anual[0]/6)
        valor_jan = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-01-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_fev = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mar = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_abr = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mai = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jun = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jul = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_ago = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_set = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_out = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_nov   = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_dez   = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)

        # df_anual_jan = df[["tipo_custo_alt","valor_custo"]][ (df['classificacao_custo']=='fixo')&(pd.to_datetime(df['dt_mes_base'])=='2024-01-01')].groupby(['tipo_custo_alt'])\
        #     .agg(jan=('valor_custo','sum')) #.sort_values("valor_custo", ascending=False)

        # df_anual_fev = df[["tipo_custo_alt","valor_custo"]][ (df['classificacao_custo']=='fixo')&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].groupby(['tipo_custo_alt'])\
        #     .agg(jan=('valor_custo','sum')) #.sort_values("valor_custo", ascending=False)
        
        # df_tp=pd.merge(df_anual, df_anual_jan, df_anual_fev, how="left", on=["tipo_custo_alt"])
        ar=[rw[0],valor_sum_anual[0], valor_med_anual,valor_jan[0],valor_fev[0],valor_mar[0],valor_abr[0],valor_mai[0],valor_jun[0],valor_jul[0],valor_ago[0],valor_set[0],valor_out[0],valor_nov[0],valor_dez[0]]
        arr_tp_ext.append(ar)

    df_tp = pd.DataFrame(arr_tp_ext, columns=['tipo','vlr_sum','vlr_med','vlr_jan','vlr_fev','vlr_mar','vlr_abr','vlr_mai','vlr_jun','vlr_jul','vlr_ago','vlr_set','vlr_out','vlr_nov','vlr_dez'])

    # df[ (df['tipo_custo_alt'])=='cabelereiro']
    #& (pd.to_datetime(df['dt_custo'])=='2024-09-02') & (df['valor_custo']==468.25) &(df['custo']=='ELECTRONATACADAO 06')]
    # df['tipo_custo'] = df.apply(lambda x: 'cabeleireiro' if x["tipo_custo"].find("cabelereiro")>=0 else x['tipo_custo'],axis=1 )

    return df_tp




def analytics_item_percent_diario(query):

    df = process(query, flag_fake=False)

    df['dia']=pd.to_datetime(df['dt_custo']).dt.day

    #list_tp=df.loc[df['classificacao_custo']=='variado'][["tipo_custo_alt"]].drop_duplicates().values.tolist()
    #list_tp_str = [''.join(i) for i in list_tp]

    arr_tp_ext=[]
    for idx, rw in enumerate(range(1,31)):
        
        #print(rw)
        valor_sum_anual = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_med_anual =  float(valor_sum_anual[0]/6)
        valor_jan = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-01-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_fev = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mar = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_abr = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mai = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jun = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jul = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_ago = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_set = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_out = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_nov   = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_dez   = df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)

        # df_anual_jan = df[["tipo_custo_alt","valor_custo"]][ (df['classificacao_custo']=='fixo')&(pd.to_datetime(df['dt_mes_base'])=='2024-01-01')].groupby(['tipo_custo_alt'])\
        #     .agg(jan=('valor_custo','sum')) #.sort_values("valor_custo", ascending=False)

        # df_anual_fev = df[["tipo_custo_alt","valor_custo"]][ (df['classificacao_custo']=='fixo')&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].groupby(['tipo_custo_alt'])\
        #     .agg(jan=('valor_custo','sum')) #.sort_values("valor_custo", ascending=False)
        
        # df_tp=pd.merge(df_anual, df_anual_jan, df_anual_fev, how="left", on=["tipo_custo_alt"])
        ar=[rw,valor_sum_anual[0], valor_med_anual,valor_jan[0],valor_fev[0],valor_mar[0],valor_abr[0],valor_mai[0],valor_jun[0],valor_jul[0],valor_ago[0],valor_set[0],valor_out[0],valor_nov[0],valor_dez[0]]
        arr_tp_ext.append(ar)

    df_tp = pd.DataFrame(arr_tp_ext, columns=['dia','vlr_sum','vlr_med','vlr_jan','vlr_fev','vlr_mar','vlr_abr','vlr_mai','vlr_jun','vlr_jul','vlr_ago','vlr_set','vlr_out','vlr_nov','vlr_dez'])

    # df[ (df['tipo_custo_alt'])=='cabelereiro']
    #& (pd.to_datetime(df['dt_custo'])=='2024-09-02') & (df['valor_custo']==468.25) &(df['custo']=='ELECTRONATACADAO 06')]
    # df['tipo_custo'] = df.apply(lambda x: 'cabeleireiro' if x["tipo_custo"].find("cabelereiro")>=0 else x['tipo_custo'],axis=1 )

    return df_tp
