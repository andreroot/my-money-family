import pandas as pd
import os
import datetime as dt
import re
import numpy as np
# sheets

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

def fake(df, list_tipo):

    list_tipo = df.apply(lambda x: x["tipo"], axis=1).values.tolist()

    # [l for l in list_tipo]

    dff = pd.DataFrame({"tipo":[l for l in list_tipo],
                        "vlr_mai":[0 for l in list_tipo]})
    

    dfx = pd.concat([df,dff],axis=1)

    return dfx


# df[(pd.to_datetime(df['dt_mes_base'])=='2025-06-01') & (df['tipo_custo']=='emprestimo')]

# df['valor_custo'].loc[(pd.to_datetime(df['dt_mes_base'])=='2025-06-01')&(df['tipo_custo']=='transporte_pub')] = 200
# df['valor_custo'].loc[(pd.to_datetime(df['dt_mes_base'])=='2025-06-01')&(df['tipo_custo']=='escola_futebol')] = 130
# df['valor_custo'].loc[(pd.to_datetime(df['dt_mes_base'])=='2025-06-01')&(df['tipo_custo']=='igreja')] = 200
# df['valor_custo'].loc[(pd.to_datetime(df['dt_mes_base'])=='2025-06-01')&(df['tipo_custo']=='cartao')] = 1000
# df['valor_custo'].loc[(pd.to_datetime(df['dt_mes_base'])=='2025-06-01')&(df['tipo_custo']=='emprestimo')] = 0

# df['valor_custo'] = df.apply(lambda x: 200 if ((x['tipo_custo']=='transporte_pub') and (pd.to_datetime(x['dt_mes_base'])=='2025-06-01')) 
#                                     else (130 if ((x['tipo_custo']=='escola_futebol') and (pd.to_datetime(x['dt_mes_base'])=='2025-06-01')) 
#                                     else (200 if ((x['tipo_custo']=='igreja')  and (pd.to_datetime(x['dt_mes_base'])=='2025-06-01')) 
#                                     else (1000 if ((x['tipo_custo']=='cartao')  and (pd.to_datetime(x['dt_mes_base'])=='2025-06-01')) 
#                                     else (0 if ((x['tipo_custo']=='emprestimo')  and (pd.to_datetime(x['dt_mes_base'])=='2025-06-01')) 
#                                     else x['valor_custo']) ) ) ) , axis=1)

# df['tipo_custo_alt_x']=df.apply(lambda x: 'net' if str(x['custo']).find("DA CLARO RESID")>=0 else x['tipo_custo_alt'], axis=1)
# df.loc[df['custo'].str.contains(r'DA+.+CLARO+.+RESID')==True, 'tipo_custo_alt'] = 'net'
#df.loc[df['custo'].astype(str).str.contains('DA CLARO RESID 75680788', na=False), 'tipo_custo_alt'] = 'net'

def tab_analytics(dfs, dfc, dfr):
    
    ano=2025

    def agrup_saldo(dfs, dt):
        max_data_saldo = dfs['dt_recebido'][(dfs['saldo']!=1.00) & ((pd.to_datetime(dfs['dt_mes_base'])==dt)) ].max()
        valor_saldo_ = dfs['saldo'][(pd.to_datetime(dfs['dt_mes_base'])==dt) & (pd.to_datetime(dfs['dt_recebido'])==max_data_saldo.strftime("%Y-%m-%d"))].max()
        return valor_saldo_ 

    def agrup_saldo_inicial(dfs, dt):
        min_data_saldo = dfs['dt_recebido'][(dfs['saldo']!=1.00) & ((pd.to_datetime(dfs['dt_mes_base'])==dt)) ].min()
        valor_saldo_ = dfs['saldo'][(pd.to_datetime(dfs['dt_mes_base'])==dt) & (pd.to_datetime(dfs['dt_recebido'])==min_data_saldo.strftime("%Y-%m-%d"))].max()
        return valor_saldo_ 

    def func_recebido(dfr, ano, mes):
        value = dfr['valor_recebido'].loc[(pd.to_datetime(dfr['dt_mes_base'])==f'{ano}-{mes}-01')].sum()
        return value

    # manipulação do custos
    def func_custo(dfc, ano, mes):
        if mes in (7,8,9):
            # Para os meses de Junho, Julho, Agosto e Setembro, adiciona 3000 ao custo
            # Isso é uma regra de negócio específica, talvez para cobrir custos extras nesses meses
            value = dfc['valor_custo'].loc[(pd.to_datetime(dfc['dt_mes_base'])==f'{ano}-{mes}-01')].sum()+3000
        else:
            value = dfc['valor_custo'].loc[(pd.to_datetime(dfc['dt_mes_base'])==f'{ano}-{mes}-01')].sum()
        return value
    
    def func_saldo(dfs, ano, mes):
        if mes==0:
            value = -1866 #agrup_saldo_inicial(dfs, f'{ano}-01-01') #9711.04
        else:
            value = agrup_saldo(dfs, f'{ano}-{mes}-01')
            #value = value -func_poupanca(dfr, mes-1)
        return value
    
    def func_poupanca(dfr, ano, mes):
        value = dfr['valor_recebido'].loc[(pd.to_datetime(dfr['dt_mes_base'])==f'{ano}-{mes}-01') & (dfr['descricao']=='TBI 9293.22915-0/500')].sum()
        return value
    
    def func_calc_fin(dfs, dfc, dfr, ano, mes):
        value = ((func_saldo(dfs, ano, mes-1)+func_recebido(dfr, ano, mes))-func_custo(dfc, ano, mes))

        return value
    
    def func_analise(dfs, dfc, dfr, ano, mes):

        value=(0 if ( func_calc_fin(dfs, dfc, dfr, ano, mes) < 0 and func_saldo(dfs, ano, mes) < 0 ) else 1)
        if value==0:
            msg='Negativo'
        else:
            msg='Positivo'
        
        return msg
    
    # resultado_202412=(msg_negativa if ( calc_financeiro_202412 < 0 and valor_saldo_202412 < 0 ) else msg)
    df_ = pd.DataFrame()
    df_ = pd.DataFrame({'analise':[ func_analise(dfs, dfc, dfr, ano, s) for s in range(1,10) ],
    'valor_saldo_inicial':[ func_saldo(dfs, ano, s-1) for s in range(1,10) ],
    'valor_poupanca':[ func_poupanca(dfr, ano, s) for s in range(1,10) ],
     'valor_recebido':[ func_recebido(dfr, ano, s)-func_poupanca(dfr, ano, s) for s in range(1,10) ],
     'valor_custo':[ func_custo(dfc, ano, s)*-1 for s in range(1,10) ],
    'valor_saldo_final':[func_saldo(dfs, ano, s)  for s in range(1,10) ],
     'recebido-custo':[ func_recebido(dfr, ano, s)-func_custo(dfc, ano, s)-func_poupanca(dfr, ano, s)  for s in range(1,10) ],
      'custo+saldo':[ (func_custo(dfc, ano, s)*-1)+func_saldo(dfs, ano, s-1) 
                     if func_saldo(dfs, ano, s-1)<0 else ((func_saldo(dfs, ano, s-1)-func_custo(dfc, ano, s)) 
                                                          if func_saldo(dfs, ano, s-1) > func_custo(dfc, ano, s) else (func_custo(dfc, ano, s)-func_saldo(dfs, ano, s-1))*-1 ) for s in range(1,10) ],
     'saldo_fake':[ func_calc_fin(dfs, dfc, dfr, ano, s) for s in range(1,10) ],
    'dt_mes_base':[pd.Timestamp(f"2025-0{s}-01") if s<=9 else pd.Timestamp(f"2025-{s}-01")  for s in range(1,10) ]
    })
 

    return df_



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
  
    lista_tp=['#2024_2023','igreja','consorcio','boleto','banco','claro','ajuda','emprestimo','cartao','net','financiamento','gas','luz','Taxa_banco','escola_futebol','juros','celular','transporte','transporte_pub','#2022','enel','carro','saque','comgas','oferta','sabesp','doação','aplicacao','seminario','cartão','creche','poupança','psicologa','ceforte','sptransp','escola','poupanca']

    list_tp_alto=['financiamento','emprestimo','boleto','cartao','juros','banco','Taxa_banco','saque','poupanca','consorcio']
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


def fake( dt_base, dt_custo, tipo_custo, custo, valor_custo):
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
        'dt_mes_base':[pd.Timestamp(datetime.datetime.strptime(dt_base, '%Y-%m-%d').date()).date() for x in range(1,len(tipo_custo)+1)],
        'dt_custo':[pd.Timestamp((datetime.datetime.strptime(dt_custo, '%Y-%m-%d')).date()).date() for x in range(1,len(tipo_custo)+1)],
        'process_time':[pd.Timestamp(datetime.datetime.strftime(dt_time_atual, '%Y-%m-%d %T.%f')) for x in range(1,len(tipo_custo)+1)]
        })

    df_fake=criar_calendario(df_fake)

    # validacao 

    nulos=df_fake[(df_fake["tipo_custo"].isnull())][['custo','valor_custo','dt_custo']].drop_duplicates().values.tolist()

    if len(nulos):
        print('verificar nulos:',nulos)
        
        df_fake=df_fake[~(df_fake["tipo_custo"].isnull())].copy()

    df_fake = construcao_tipo_custo(df_fake)
    #['tipo_custo','custo','valor_custo','dt_mes_base','dt_custo','tipo_custo_alt','classificacao_custo','area_custo']
    #['tipo_custo', 'custo', 'valor_custo', 'dt_mes_base', 'dt_custo', 'tipo_custo_alt', 'classificacao_custo',       'area_custo']
    return df_fake[['tipo_custo','custo','valor_custo','dt_mes_base','dt_custo','tipo_custo_alt','classificacao_custo','area_custo']]
