
from v0.resource.fin_api import consulta_gcp

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

def construcao_tipo_custo(df):

    df["tipo_custo_alt"]=df.apply(lambda x: ''.join(re.findall('^\w+',x["tipo_custo"])), axis=1)
  
    lista_tp=['#2024_2023','igreja','boleto','banco','claro','ajuda','emprestimo','cartao','net','financiamento','gas','luz','Taxa_banco','escola_futebol','juros','celular','transporte','transporte_pub','#2022','enel','carro','saque','comgas','oferta','sabesp','doação','aplicacao','seminario','cartão','creche','poupança','psicologa','ceforte','sptransp','escola','poupanca']

    list_tp_alto=['financiamento','emprestimo','boleto','cartao','juros','banco','Taxa_banco','saque','poupanca']
    list_tp_medio=['luz','gas','net','sabesp','claro']
    list_tp_medio_n1=['pix', 'transporte', 'alimentacao','feira','mercado','padaria','transporte_pub','recargapay','cea','ifood','cabelereiro','farmacia','roupa','ajuda','pernambucanas','dora']
    list_tp_invest=['picpay','inter','nubank','pagbank','nomad']# remover poupanca
    list_tp_imprevisto=['pedreiro','consertotv','refil','flores','cartorio','shopmetro','lojaesportes','utensilios','app']
    list_tp_lazer=['brinquedo-praia','lazer','casamento','cinema','brinquedo','passeio','futebol','escola_futebol','churrasco-pessoal','futebol_amigos']
    list_tp_comercio=['material_construcao_banheiro','celular','eletrolux','pessoa','loja','cosmeticos','comercio','BAZAR','kalung','americanas',  'refil', 'comercio',  'kalung',  'loja', 'papelaria',  'presente',  'lojaesportes',  'brinquedo', 'amigosecreto', 'consumo', 'panetone', 'correio', 'chuteira', 'material_construcao_banheiro', 'eletrolux', 'consertotv', 'cinema',  'cosmeticos',  'shopmetro']
    list_tp_carro=['porto_seguro','multa','uber','estacionamento','carro','pedagio','ipva','gasolina','mecanico','locadora']
    list_tp_igreja=['igreja']
    list_tp_pessoa=['rafael','ajuda','dora']


    df["classificacao_custo"] = df.apply(lambda x: 'fixo' if [tp for tp in lista_tp if tp==x["tipo_custo_alt"]] else 'variado', axis=1)

    df["area_custo"] = df.apply(lambda x: 'alto' if [tp for tp in list_tp_alto if tp==x["tipo_custo_alt"] ] else 
                                ('medio' if [tp for tp in list_tp_medio if tp==x["tipo_custo_alt"]] else 
                                ('medio_n1' if [tp for tp in list_tp_medio_n1 if tp==x["tipo_custo_alt"]] else 
                                    ('invest' if [tp for tp in list_tp_invest if tp==x["tipo_custo_alt"]] else
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

    df = construcao_tipo_custo(df)

    # tratar gasto pessoal
    df['tipo_custo_alt'] = df.apply(lambda x: 'churrasco-pessoal' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
                                                                    x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-09-01' and
                                                                    x["dt_custo"].strftime('%Y-%m-%d')=='2024-09-02' and
                                                                    x["valor_custo"]==468.25  else x['tipo_custo_alt'], axis=1)

    df['tipo_custo'] = df.apply(lambda x: 'churrasco-pessoal' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
                                                                    x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-09-01' and
                                                                    x["dt_custo"].strftime('%Y-%m-%d')=='2024-09-02' and
                                                                    x["valor_custo"]==468.25  else x['tipo_custo'], axis=1)

    # tratar gasto pessoal
    df['tipo_custo_alt'] = df.apply(lambda x: 'churrasco-futebolamigos' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
                                                                    x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-12-01' and
                                                                    x["dt_custo"].strftime('%Y-%m-%d')=='2024-12-13' and
                                                                    x["valor_custo"]==657.10		  else x['tipo_custo_alt'], axis=1)

    df['tipo_custo'] = df.apply(lambda x: 'churrasco-futebolamigos' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
                                                                    x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-12-01' and
                                                                    x["dt_custo"].strftime('%Y-%m-%d')=='2024-12-13' and
                                                                    x["valor_custo"]==657.10	  else x['tipo_custo'], axis=1)


    # tratar gasto pessoal
    df['tipo_custo_alt'] = df.apply(lambda x: 'ceia-fimdeano' if (x["custo"].find("ELECTRONHIP BERGAMI")>=0 or x["custo"].find("ELECTRONATACADAO 06")>=0 or x["custo"].find("ELECTRONRS AVILA")>=0) and
                                                                    x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-12-01' and
                                                                    x["dt_custo"].strftime('%Y-%m-%d') in ('2024-12-23','2024-12-24','2024-12-30','2024-12-31') and
                                                                    x["valor_custo"] in (225.03,185.81,355.66,94.61)		  else x['tipo_custo_alt'], axis=1)

    df['tipo_custo'] = df.apply(lambda x: 'ceia-fimdeano' if (x["custo"].find("ELECTRONHIP BERGAMI")>=0 or x["custo"].find("ELECTRONATACADAO 06")>=0 or x["custo"].find("ELECTRONRS AVILA")>=0) and
                                                                    x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-12-01' and
                                                                    x["dt_custo"].strftime('%Y-%m-%d') in ('2024-12-23','2024-12-24','2024-12-30','2024-12-31') and
                                                                    x["valor_custo"] in (225.03,185.81,355.66,94.61)		  else x['tipo_custo'], axis=1)
    
    df['tipo_custo_alt']=df.apply(lambda x: 'banco' if  x['custo'].find('TAR PACOTE ITAU')>=0 
                                  else ('Taxa_banco' if x['custo'].find('IOF')>=0 
                                        else ( 'juros' if x['custo'].find('JUROS LIMITE DA CONTA')>=0 else   x['tipo_custo_alt'])), axis=1)

    df['tipo_custo']=df.apply(lambda x: 'banco' if  x['custo'].find('TAR PACOTE ITAU')>=0 
                              else ('Taxa_banco' if x['custo'].find('IOF')>=0 
                                    else ( 'juros' if x['custo'].find('JUROS LIMITE DA CONTA')>=0 else   x['tipo_custo'])), axis=1)


    return df

# ANALYTICS MENSAL PARA FIXO E VARIAVEL

def analytics_percent_geral(df):

    #df = process(query, flag_fake=False)

    list_dt=df[['dt_mes_base']].sort_values("dt_mes_base", ascending=True).drop_duplicates().values.tolist()

    def get_valor_custo(df, tp, dt):
        return df[["valor_custo"]][ (df['tipo_custo_alt']==tp)&(pd.to_datetime(df['dt_mes_base'])==dt)].sum().values.tolist()

    # LISTAGEM DO TIPO DE CUSTO
    list_tp=df[["tipo_custo_alt"]].drop_duplicates().values.tolist()
    
    arr_tp_ext=[]

    valor_jan_sum = 0
    valor_fev_sum = 0
    valor_mar_sum = 0
    valor_abr_sum = 0
    valor_mai_sum = 0
    valor_jun_sum = 0
    valor_jul_sum = 0
    valor_ago_sum = 0
    valor_set_sum = 0
    valor_out_sum = 0
    valor_nov_sum = 0
    valor_dez_sum = 0

    valor_med_anual_sum = 0
    valor_sum_anual_sum = 0
    
    for idx, rw in enumerate(list_tp):

        valor_sum_anual = df[["valor_custo"]][ (df['tipo_custo_alt']==rw[0])].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_med_anual =  float(valor_sum_anual[0]/len(list_dt))

        valor_jan   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[0][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False)
        valor_fev   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[1][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mar   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[2][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_abr   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[3][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mai   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[4][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jun   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[5][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jul   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[6][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_ago   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[7][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_set   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[8][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_out   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[9][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_nov   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[10][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_dez   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[11][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)

        ar=[rw[0],valor_sum_anual[0],valor_med_anual,valor_jan[0],valor_fev[0],valor_mar[0],valor_abr[0],valor_mai[0],valor_jun[0],valor_jul[0],valor_ago[0],valor_set[0],valor_out[0],valor_nov[0], valor_dez[0]]
        arr_tp_ext.append(ar)

        valor_jan_sum = valor_jan_sum + valor_jan[0]
        valor_fev_sum = valor_fev_sum + valor_fev[0]
        valor_mar_sum = valor_mar_sum + valor_mar[0]
        valor_abr_sum = valor_abr_sum + valor_abr[0]
        valor_mai_sum = valor_mai_sum + valor_mai[0]
        valor_jun_sum = valor_jun_sum + valor_jun[0]
        valor_jul_sum = valor_jul_sum + valor_jul[0]
        valor_ago_sum = valor_ago_sum + valor_ago[0]
        valor_set_sum = valor_set_sum + valor_set[0]
        valor_out_sum = valor_out_sum + valor_out[0]
        valor_nov_sum = valor_nov_sum + valor_nov[0]
        valor_dez_sum = valor_dez_sum + valor_dez[0]
        valor_med_anual_sum = valor_med_anual_sum + valor_med_anual
        valor_sum_anual_sum = valor_sum_anual_sum + valor_sum_anual[0]

    ar=['Total', valor_sum_anual_sum, valor_med_anual_sum, valor_jan_sum, valor_fev_sum, valor_mar_sum, valor_abr_sum, valor_mai_sum, valor_jun_sum, valor_jul_sum, valor_ago_sum, valor_set_sum, valor_out_sum, valor_nov_sum, valor_dez_sum]
    arr_tp_ext.append(ar)

    df_tp = pd.DataFrame(arr_tp_ext, columns=['tipo','vlr_sum','vlr_med','vlr_jan','vlr_fev','vlr_mar','vlr_abr','vlr_mai','vlr_jun','vlr_jul','vlr_ago','vlr_set','vlr_out','vlr_nov','vlr_dez'])

    return df_tp

def analytics_percent(df, classif):

    #df = process(query, flag_fake=False)

    list_dt=df[['dt_mes_base']].sort_values("dt_mes_base", ascending=True).drop_duplicates().values.tolist()

    def get_valor_custo(df, tp, dt):
        return df[["valor_custo"]][ (df['classificacao_custo']==classif) & (df['tipo_custo_alt']==tp)&(pd.to_datetime(df['dt_mes_base'])==dt)].sum().values.tolist()

    # LISTAGEM DO TIPO DE CUSTO
    list_tp=df.loc[(df['classificacao_custo']==classif)][["tipo_custo_alt"]].drop_duplicates().values.tolist()
    
    arr_tp_ext=[]

    valor_jan_sum = 0
    valor_fev_sum = 0
    valor_mar_sum = 0
    valor_abr_sum = 0
    valor_mai_sum = 0
    valor_jun_sum = 0
    valor_jul_sum = 0
    valor_ago_sum = 0
    valor_set_sum = 0
    valor_out_sum = 0
    valor_nov_sum = 0
    valor_dez_sum = 0

    valor_med_anual_sum = 0
    valor_sum_anual_sum = 0
    
    for idx, rw in enumerate(list_tp):

        valor_sum_anual = df[["valor_custo"]][ (df['classificacao_custo']==classif) & (df['tipo_custo_alt']==rw[0])].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_med_anual =  float(valor_sum_anual[0]/len(list_dt))

        valor_jan   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[0][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False)
        valor_fev   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[1][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mar   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[2][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_abr   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[3][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mai   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[4][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jun   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[5][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jul   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[6][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_ago   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[7][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_set   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[8][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_out   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[9][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_nov   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[10][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_dez   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[11][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)

        ar=[rw[0],valor_sum_anual[0],valor_med_anual,valor_jan[0],valor_fev[0],valor_mar[0],valor_abr[0],valor_mai[0],valor_jun[0],valor_jul[0],valor_ago[0],valor_set[0],valor_out[0],valor_nov[0], valor_dez[0]]
        arr_tp_ext.append(ar)

        valor_jan_sum = valor_jan_sum + valor_jan[0]
        valor_fev_sum = valor_fev_sum + valor_fev[0]
        valor_mar_sum = valor_mar_sum + valor_mar[0]
        valor_abr_sum = valor_abr_sum + valor_abr[0]
        valor_mai_sum = valor_mai_sum + valor_mai[0]
        valor_jun_sum = valor_jun_sum + valor_jun[0]
        valor_jul_sum = valor_jul_sum + valor_jul[0]
        valor_ago_sum = valor_ago_sum + valor_ago[0]
        valor_set_sum = valor_set_sum + valor_set[0]
        valor_out_sum = valor_out_sum + valor_out[0]
        valor_nov_sum = valor_nov_sum + valor_nov[0]
        valor_dez_sum = valor_dez_sum + valor_dez[0]
        valor_med_anual_sum = valor_med_anual_sum + valor_med_anual
        valor_sum_anual_sum = valor_sum_anual_sum + valor_sum_anual[0]

    ar=['Total', valor_sum_anual_sum, valor_med_anual_sum, valor_jan_sum, valor_fev_sum, valor_mar_sum, valor_abr_sum, valor_mai_sum, valor_jun_sum, valor_jul_sum, valor_ago_sum, valor_set_sum, valor_out_sum, valor_nov_sum, valor_dez_sum]
    arr_tp_ext.append(ar)

    df_tp = pd.DataFrame(arr_tp_ext, columns=['tipo','vlr_sum','vlr_med','vlr_jan','vlr_fev','vlr_mar','vlr_abr','vlr_mai','vlr_jun','vlr_jul','vlr_ago','vlr_set','vlr_out','vlr_nov','vlr_dez'])

    return df_tp




def analytics_percent_2025(df, classif):

    #df = process(query, flag_fake=False)
    import datetime

    #list_dt=df[['dt_mes_base']].sort_values("dt_mes_base", ascending=True).drop_duplicates().values.tolist()
    
    list_dt=[[datetime.date(2025, 1, 1)],[datetime.date(2025, 2, 1)],[datetime.date(2025, 3, 1)],[datetime.date(2025, 4, 1)],[datetime.date(2025, 5, 1)],[datetime.date(2025, 6, 1)],[datetime.date(2025, 7, 1)],[datetime.date(2025, 8, 1)],[datetime.date(2025, 9, 1)],[datetime.date(2025, 10, 1)],[datetime.date(2025, 11, 1)],[datetime.date(2025, 12, 1)]]
    
    #[[datetime.date(2025, 1, 1)],[datetime.date(2025, 2, 1)]]

    def get_valor_custo(df, tp, dt):
        return df[["valor_custo"]][ (df['classificacao_custo']==classif) & (df['tipo_custo_alt']==tp)&(pd.to_datetime(df['dt_mes_base'])==dt)].sum().values.tolist()

    # LISTAGEM DO TIPO DE CUSTO
    list_tp=df.loc[(df['classificacao_custo']==classif)][["tipo_custo_alt"]].drop_duplicates().values.tolist()
    
    arr_tp_ext=[]

    valor_jan_sum = 0
    valor_fev_sum = 0
    valor_mar_sum = 0
    valor_abr_sum = 0
    valor_mai_sum = 0
    valor_jun_sum = 0
    valor_jul_sum = 0
    valor_ago_sum = 0
    valor_set_sum = 0
    valor_out_sum = 0
    valor_nov_sum = 0
    valor_dez_sum = 0

    valor_med_anual_sum = 0
    valor_sum_anual_sum = 0
    
    for idx, rw in enumerate(list_tp):

        valor_sum_anual = df[["valor_custo"]][ (df['classificacao_custo']==classif) & (df['tipo_custo_alt']==rw[0])].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_med_anual =  float(valor_sum_anual[0]/len(list_dt))

        valor_jan   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[0][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False)
        valor_fev   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[1][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mar   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[2][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_abr   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[3][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mai   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[4][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jun   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[5][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jul   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[6][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_ago   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[7][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_set   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[8][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_out   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[9][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_nov   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[10][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_dez   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[11][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)

        ar=[rw[0],valor_sum_anual[0],valor_med_anual,valor_jan[0],valor_fev[0],valor_mar[0],valor_abr[0],valor_mai[0],valor_jun[0],valor_jul[0],valor_ago[0],valor_set[0],valor_out[0],valor_nov[0], valor_dez[0]]
        arr_tp_ext.append(ar)

        valor_jan_sum = valor_jan_sum + valor_jan[0]
        valor_fev_sum = valor_fev_sum + valor_fev[0]
        valor_mar_sum = valor_mar_sum + valor_mar[0]
        valor_abr_sum = valor_abr_sum + valor_abr[0]
        valor_mai_sum = valor_mai_sum + valor_mai[0]
        valor_jun_sum = valor_jun_sum + valor_jun[0]
        valor_jul_sum = valor_jul_sum + valor_jul[0]
        valor_ago_sum = valor_ago_sum + valor_ago[0]
        valor_set_sum = valor_set_sum + valor_set[0]
        valor_out_sum = valor_out_sum + valor_out[0]
        valor_nov_sum = valor_nov_sum + valor_nov[0]
        valor_dez_sum = valor_dez_sum + valor_dez[0]
        valor_med_anual_sum = valor_med_anual_sum + valor_med_anual
        valor_sum_anual_sum = valor_sum_anual_sum + valor_sum_anual[0]

    ar=['Total', valor_sum_anual_sum, valor_med_anual_sum, valor_jan_sum, valor_fev_sum, valor_mar_sum, valor_abr_sum, valor_mai_sum, valor_jun_sum, valor_jul_sum, valor_ago_sum, valor_set_sum, valor_out_sum, valor_nov_sum, valor_dez_sum]
    arr_tp_ext.append(ar)

    df_tp = pd.DataFrame(arr_tp_ext, columns=['tipo','vlr_sum','vlr_med','vlr_jan','vlr_fev','vlr_mar','vlr_abr','vlr_mai','vlr_jun','vlr_jul','vlr_ago','vlr_set','vlr_out','vlr_nov','vlr_dez'])

    return df_tp
# ANALYTICS DIARIA FIXO E VARIAVEL

def analytics_item_percent_diario(df, classif, tipo_custo):

    #df = process(query, flag_fake=False)

    list_dt=df[['dt_mes_base']].sort_values("dt_mes_base", ascending=True).drop_duplicates().values.tolist()

    def get_valor_custo(df, dia, dt):
        #return df[["valor_custo"]][ (df['classificacao_custo']==classif) & (df['tipo_custo_alt']==tp)&(pd.to_datetime(df['dt_mes_base'])==dt)].sum().values.tolist()
        return df[["valor_custo"]][ (df['classificacao_custo']==classif) & (df['tipo_custo_alt']==tipo_custo) & (df['dia']==dia)&(pd.to_datetime(df['dt_mes_base'])==dt)].sum().values.tolist() 

    df['dia']=pd.to_datetime(df['dt_custo']).dt.day

    arr_tp_ext=[]

    valor_jan_sum = 0
    valor_fev_sum = 0
    valor_mar_sum = 0
    valor_abr_sum = 0
    valor_mai_sum = 0
    valor_jun_sum = 0
    valor_jul_sum = 0
    valor_ago_sum = 0
    valor_set_sum = 0
    valor_out_sum = 0
    valor_nov_sum = 0
    valor_dez_sum = 0

    valor_med_anual_sum = 0
    valor_sum_anual_sum = 0

    for idx, rw in enumerate(range(1,31)):
        
        valor_sum_anual = df[["valor_custo"]][ (df['classificacao_custo']==classif) & (df['tipo_custo_alt']==tipo_custo) & (df['dia']==rw)].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_med_anual =  float(valor_sum_anual[0]/len(list_dt))

        valor_jan = get_valor_custo(df, rw, dt.date.strftime(list_dt[0][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-01-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_fev = get_valor_custo(df, rw, dt.date.strftime(list_dt[1][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mar = get_valor_custo(df, rw, dt.date.strftime(list_dt[2][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_abr = get_valor_custo(df, rw, dt.date.strftime(list_dt[3][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mai = get_valor_custo(df, rw, dt.date.strftime(list_dt[4][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jun = get_valor_custo(df, rw, dt.date.strftime(list_dt[5][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jul = get_valor_custo(df, rw, dt.date.strftime(list_dt[6][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_ago = get_valor_custo(df, rw, dt.date.strftime(list_dt[7][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_set = get_valor_custo(df, rw, dt.date.strftime(list_dt[8][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_out = get_valor_custo(df, rw, dt.date.strftime(list_dt[9][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_nov = get_valor_custo(df, rw, dt.date.strftime(list_dt[10][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_dez = get_valor_custo(df, rw, dt.date.strftime(list_dt[11][0],"%Y-%m-%d")) #df[["valor_custo"]][ (df['classificacao_custo']=='variado') & (df['dia']==rw)&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        
        ar=[rw, valor_sum_anual[0], valor_med_anual, valor_jan[0], valor_fev[0], valor_mar[0], valor_abr[0], valor_mai[0], valor_jun[0], valor_jul[0], valor_ago[0],valor_set[0],valor_out[0],valor_nov[0],valor_dez[0]]
        arr_tp_ext.append(ar)
    
        valor_jan_sum = valor_jan_sum + valor_jan[0]
        valor_fev_sum = valor_fev_sum + valor_fev[0]
        valor_mar_sum = valor_mar_sum + valor_mar[0]
        valor_abr_sum = valor_abr_sum + valor_abr[0]
        valor_mai_sum = valor_mai_sum + valor_mai[0]
        valor_jun_sum = valor_jun_sum + valor_jun[0]
        valor_jul_sum = valor_jul_sum + valor_jul[0]
        valor_ago_sum = valor_ago_sum + valor_ago[0]
        valor_set_sum = valor_set_sum + valor_set[0]
        valor_out_sum = valor_out_sum + valor_out[0]
        valor_nov_sum = valor_nov_sum + valor_nov[0]
        valor_dez_sum = valor_dez_sum + valor_dez[0]
        valor_med_anual_sum = valor_med_anual_sum + valor_med_anual
        valor_sum_anual_sum = valor_sum_anual_sum + valor_sum_anual[0]

    ar=['Total', valor_sum_anual_sum, valor_med_anual_sum, valor_jan_sum, valor_fev_sum, valor_mar_sum, valor_abr_sum, valor_mai_sum, valor_jun_sum, valor_jul_sum, valor_ago_sum, valor_set_sum, valor_out_sum, valor_nov_sum, valor_dez_sum]
    arr_tp_ext.append(ar)

    df_tp = pd.DataFrame(arr_tp_ext, columns=['dia','vlr_sum','vlr_med','vlr_jan','vlr_fev','vlr_mar','vlr_abr','vlr_mai','vlr_jun','vlr_jul','vlr_ago','vlr_set','vlr_out','vlr_nov','vlr_dez'])

    return df_tp



# ANALYTICS MENSAL PARA FIXO E VARIAVEL

def analytics_receb_percent(df, ano_base):

    #df = process(query, flag_fake=False) valor_recebido	dt_mes_base

    list_dt=df[['dt_mes_base']].sort_values("dt_mes_base", ascending=True).drop_duplicates().values.tolist()

    def get_valor_custo(df, dt):
        return df[["valor_recebido"]][ (pd.to_datetime(df['dt_mes_base'])==dt)].sum().values.tolist()
    
    arr_tp_ext=[]

    valor_jan_sum = 0
    valor_fev_sum = 0
    valor_mar_sum = 0
    valor_abr_sum = 0
    valor_mai_sum = 0
    valor_jun_sum = 0
    valor_jul_sum = 0
    valor_ago_sum = 0
    valor_set_sum = 0
    valor_out_sum = 0
    valor_nov_sum = 0
    valor_dez_sum = 0

    valor_med_anual_sum = 0
    valor_sum_anual_sum = 0
    
    valor_sum_anual = df[["valor_recebido"]].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_med_anual =  float(valor_sum_anual[0]/len(list_dt))

    valor_jan   = get_valor_custo(df, dt.date.strftime(list_dt[0][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False)
    valor_fev   = get_valor_custo(df, dt.date.strftime(list_dt[1][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_mar   = get_valor_custo(df, dt.date.strftime(list_dt[2][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_abr   = get_valor_custo(df, dt.date.strftime(list_dt[3][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_mai   = get_valor_custo(df, dt.date.strftime(list_dt[4][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_jun   = get_valor_custo(df, dt.date.strftime(list_dt[5][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_jul   = get_valor_custo(df, dt.date.strftime(list_dt[6][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_ago   = get_valor_custo(df, dt.date.strftime(list_dt[7][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_set   = get_valor_custo(df, dt.date.strftime(list_dt[8][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_out   = get_valor_custo(df, dt.date.strftime(list_dt[9][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_nov   = get_valor_custo(df, dt.date.strftime(list_dt[10][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
    valor_dez   = get_valor_custo(df, dt.date.strftime(list_dt[11][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)

    ar=[ano_base,valor_sum_anual[0],valor_med_anual,valor_jan[0],valor_fev[0],valor_mar[0],valor_abr[0],valor_mai[0],valor_jun[0],valor_jul[0],valor_ago[0],valor_set[0],valor_out[0],valor_nov[0], valor_dez[0]]
    arr_tp_ext.append(ar)

    # valor_jan_sum = valor_jan_sum + valor_jan[0]
    # valor_fev_sum = valor_fev_sum + valor_fev[0]
    # valor_mar_sum = valor_mar_sum + valor_mar[0]
    # valor_abr_sum = valor_abr_sum + valor_abr[0]
    # valor_mai_sum = valor_mai_sum + valor_mai[0]
    # valor_jun_sum = valor_jun_sum + valor_jun[0]
    # valor_jul_sum = valor_jul_sum + valor_jul[0]
    # valor_ago_sum = valor_ago_sum + valor_ago[0]
    # valor_set_sum = valor_set_sum + valor_set[0]
    # valor_out_sum = valor_out_sum + valor_out[0]
    # valor_nov_sum = valor_nov_sum + valor_nov[0]
    # valor_dez_sum = valor_dez_sum + valor_dez[0]
    # valor_med_anual_sum = valor_med_anual_sum + valor_med_anual
    # valor_sum_anual_sum = valor_sum_anual_sum + valor_sum_anual[0]

    #ar=['Total', valor_sum_anual_sum, valor_med_anual_sum, valor_jan_sum, valor_fev_sum, valor_mar_sum, valor_abr_sum, valor_mai_sum, valor_jun_sum, valor_jul_sum, valor_ago_sum, valor_set_sum, valor_out_sum, valor_nov_sum, valor_dez_sum]
    #arr_tp_ext.append(ar)

    df_tp = pd.DataFrame(arr_tp_ext, columns=['ano_base','vlr_sum','vlr_med','vlr_jan','vlr_fev','vlr_mar','vlr_abr','vlr_mai','vlr_jun','vlr_jul','vlr_ago','vlr_set','vlr_out','vlr_nov','vlr_dez'])

    return df_tp