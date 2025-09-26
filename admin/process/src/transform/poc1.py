

import pandas as pd   
import datetime as dt
import re
from datetime import timedelta, date


def func_analise(saldo, receb, prev, mes):
    # Verifica se o saldo inicial e o custo financeiro são negativos
    # Se ambos forem negativos, retorna 'Negativo', caso contrário, 'Positivo'
    # PREVISAO
    if mes>=dt.date.strftime(dt.datetime.now(),"%B"):  
        if prev>0:
            if saldo<0:
                msg='ATENÇÃO PREV FUTURO Saldo inicial Negativo é um alerta! ENTRADA ACIMA da média. Seus gastos PRECISAM SER planejados, para seu saldo final esta sem risco!'
            else:
                msg='ATENÇÃO PREV FUTURO Saldo inicial Negativo é um alerta! ENTRADA ABAIXO da média. Seus gastos PRECISAM SER planejados, para seu saldo final esta sem risco!'

        else:
            msg='ATENÇÃO PREV FUTURO Saldo inicial POSITIVO é muito bom! Mas seus gastos NÃO ESTÃO SENDO BEM PLANEJADOS, para seu saldo final esta sem risco!'
            #msg='ATENÇÃO PREV FUTURO Saldo inicial POSITIVO é muito bom! Mas seus gastos PRECISAM SER planejados, para seu saldo final esta sem risco!'

    else:      
        if saldo<0:
            if  receb>10000:
                msg='Saldo inicial Negativo mas Teve uma ENTRADA acima da media, seu saldo final pode ficar fora de risco!'
            else:
                # PREVISAO
                msg='Saldo inicial Negativo é um alerta! ENTRADA ABAIXO da média. Seus gastos PRECISAM SER planejados, para seu saldo final esta sem risco!'

        else:
            if saldo<0:
                msg='Saldo inicial Positivo muito bom! Mas seus gastos não foram bem planejados e seu saldo final em risco!'
            else:
                if  receb>10000:
                    msg='Saldo inicial Positivo muito bom! Teve uma ENTRADA acima da media, seu saldo final esta fora de risco!'
                else:
                    msg='Saldo inicial Positivo muito bom! Seus gastos PRECISAM SER planejados, para seu saldo final esta sem risco!'

    return msg

def calculate_prevision(df):
    def func(mes,columns):
        data_json = df[df['mes_base']==mes].to_dict(orient="records")
        return data_json[0][columns]
    #
    # print(func('October','saldo_inicial'))

    # # data_json = df.to_dict(orient="records")
    def func_nov_saldo_i():
        return func('October','saldo_inicial')+func('October','saida')+func('October','entrada')

    def func_nov_saldo_f():
        return func('October','saldo_inicial')+func('October','saida')+func('October','entrada')

    def saldo_inicial(mes):
        if mes=='November':
            return func('October','saldo_final')
        elif mes=='December':
            return func('October','saldo_final')+func('November','entrada')+func('November','saida')
        else:
            return func(mes,'saldo_inicial')

    def entrada(mes):
        return func(mes,'entrada')  

    def saida(mes):
        return func(mes,'saida')

    def prev(mes):
        return func(mes,'prev_saldo_final')

    def saldo_final(mes):
        if mes=='November':
            return func('October','saldo_final')+func('November','entrada')+func('November','saida')
        elif mes=='December':
            return func('October','saldo_final')+func('November','entrada')+func('November','saida')+func(mes,'entrada')+func(mes,'saida')
        else:
            return func(mes,'saldo_final')
    

    list_mes = [dt.date.strftime(pd.Timestamp(f"2025-0{mes}-01"),"%B") if mes<=9 else dt.date.strftime(pd.Timestamp(f"2025-{mes}-01"),"%B") for mes in range(10,13)]

    # resultado_202412=(msg_negativa if ( calc_financeiro_202412 < 0 and valor_saldo_202412 < 0 ) else msg)
    df_ = pd.DataFrame()
    df_ = pd.DataFrame({
        'analise':['' for i, mes in enumerate(list_mes)],
        # 'analise':[data_json[i]['analise'] for i, mes in enumerate(list_mes)],
        'saldo_inicial': [saldo_inicial(mes) for i, mes in enumerate(list_mes)],
        'entrada':[entrada(mes) for i, mes in enumerate(list_mes)],
        'saida':[saida(mes) for i, mes in enumerate(list_mes)],
        'saldo_final':[saldo_final(mes) for i, mes in enumerate(list_mes)],
        'prev_saldo_final':[ saldo_final(mes) for i, mes in enumerate(list_mes)],
        'mes_base':[mes+'analytics' for mes in list_mes]
    })    
    print(df_)
    #-R$ 1.111,60	R$ 31.000,00	-R$ 5.527,41	R$ 24.434,32	R$ 24.434,32
    return df_

def calculate_analytics(dfs, dfc, dfr):

    def agrup_saldo_inicial(dfs, database):
        min_data_saldo = dfs['dt_receb'][(dfs['valor_saldo']!=1.00) & ((pd.to_datetime(dfs['data_base'])==database)) ].min()
        valor_saldo_ = dfs['valor_saldo'][(pd.to_datetime(dfs['data_base'])==database) & (pd.to_datetime(dfs['dt_receb'])==min_data_saldo)].max()
        
        return valor_saldo_ 

    def agrup_saldo(dfs, database):
        max_data_saldo = dfs['dt_receb'][(dfs['valor_saldo']!=1.00) & ((pd.to_datetime(dfs['data_base'])==database)) ].max()
        valor_saldo_ = dfs['valor_saldo'][(pd.to_datetime(dfs['data_base'])==database) & (pd.to_datetime(dfs['dt_receb'])==max_data_saldo)].max()
        return valor_saldo_

    def func_saldo(dfs, mes, flag_inicial=True):
        # if mes==0:
        #     # value = -4631.68 #agrup_saldo_inicial(dfs, f'{ano}-01-01') #9711.04
        #     database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")
        #     value = agrup_saldo(dfs, database)            
        # else:
        database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")
        
        if flag_inicial:
            value = agrup_saldo_inicial(dfs, database)
        else:
            value = agrup_saldo(dfs, database)

        return value

    def func_prev_saldo(dfs, dfc, dfr, mes):
        value = func_prev_analytics(dfs, dfc, dfr, mes)-func_saldo(dfs, mes-1, True) #((func_prev_analytics(dfs, dfc, dfr, mes)+func_recebido(dfr, mes))+func_custo(dfc, mes))
        return value

    def func_recebido(dfr, mes):
        database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")
        # MES DO RECEBIMENTO DA RECISÃO DE CONTRATO
        if mes==10:
            value = dfr['valor_receb'].loc[(pd.to_datetime(dfr['data_base'])==database)].sum()+30000
        else:
            value = dfr['valor_receb'].loc[(pd.to_datetime(dfr['data_base'])==database)].sum()
        return value

    # manipulação do custos
    def func_custo(dfc, mes):
        database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")

        if mes>=dt.datetime.now().month:
            # Para os meses de fake adiciona 3000 ao custo
            # Isso é uma regra de negócio específica, talvez para cobrir custos extras nesses meses
            value = dfc['valor_custo'].loc[(pd.to_datetime(dfc['data_base'])==database)].sum()+2500
        else:
            value = dfc['valor_custo'].loc[(pd.to_datetime(dfc['data_base'])==database)].sum()
        return value
    

    def func_prev_analytics(dfs, dfc, dfr, mes):
        if mes>=dt.datetime.now().month:
            if mes==10:
                print(f"DEBUG: func_prev_analytics called for month {mes}")
                # pegr saldo inicial do mes simular um gasto medio e uma previsão de entrada 
                value = ((func_saldo(dfs, mes, True)+func_recebido(dfr, mes))-func_custo(dfc, mes))
            else:
                value = ((func_saldo(dfs, mes, True)+func_recebido(dfr, mes))-func_custo(dfc, mes))
        else:
            value = 0
        return value
    
    
    # resultado_202412=(msg_negativa if ( calc_financeiro_202412 < 0 and valor_saldo_202412 < 0 ) else msg)
    df_ = pd.DataFrame()
    df_ = pd.DataFrame({
        'analise':['' for mes in range(1,13)],
        'saldo_inicial': [func_saldo(dfs, mes, True) if mes < 10 else func_prev_analytics(dfs, dfc, dfr, mes-1) for mes in range(1,13)],
        'entrada':[ func_recebido(dfr, mes) for mes in range(1,13)],
        'saida':[ func_custo(dfc, mes)*-1 for mes in range(1,13) ],
        'saldo_final':[func_saldo(dfs, mes, False) if mes < 10 else func_prev_saldo(dfs, dfc, dfr, mes) for mes in range(1,13) ],
        'prev_saldo_final':[ func_prev_analytics(dfs, dfc, dfr, mes) if mes < 10 else func_prev_saldo(dfs, dfc, dfr, mes) for mes in range(1,13) ],
        'mes_base':[dt.date.strftime(pd.Timestamp(f"2025-0{mes}-01"),"%B") if mes<=9 else dt.date.strftime(pd.Timestamp(f"2025-{mes}-01"),"%B") for mes in range(1,13)]
    })
    
    df__ = calculate_prevision(df_)

    # df_ = pd.merge(df_, df__, how='left', left_on=('mes_base'), right_on='mes_base')
    df_final = pd.concat([df_.iloc[:-3], df__], ignore_index=True)


    df_final['analise'] = df_final.apply(lambda x: func_analise(x['saldo_inicial'], x['entrada'], x['prev_saldo_final'], x['mes_base']), axis=1)

    return df_final


def calculate_prevanalytics(dfs, dfc, dfr):

    # manipulação do saldo inicial
    def func_saldo_inicial(dfs, database):
        min_data_saldo = dfs['dt_receb'][(dfs['valor_saldo']!=1.00) & ((pd.to_datetime(dfs['data_base'])==database)) ].min()
        valor_saldo_ = dfs['valor_saldo'][(pd.to_datetime(dfs['data_base'])==database) & (pd.to_datetime(dfs['dt_receb'])==min_data_saldo)].max()
        print(f"DEBUG: func_saldo_inicial called for date {database}, returning {valor_saldo_}")
        return valor_saldo_ 
    
    # manipulação do recebido
    def func_recebido(dfr, mes):
        database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")
        # MES DO RECEBIMENTO DA RECISÃO DE CONTRATO
        if mes==10:
            value = dfr['valor_receb'].loc[(pd.to_datetime(dfr['data_base'])==database)].sum()+30000
        else:
            value = dfr['valor_receb'].loc[(pd.to_datetime(dfr['data_base'])==database)].sum()
        return value


    # manipulação do custos
    def func_custo(dfc, mes):
        database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")

        if mes>=dt.datetime.now().month:
            # Para os meses de fake adiciona 3000 ao custo
            # Isso é uma regra de negócio específica, talvez para cobrir custos extras nesses meses
            value = dfc['valor_custo'].loc[(pd.to_datetime(dfc['data_base'])==database)].sum()+2500
        else:
            value = dfc['valor_custo'].loc[(pd.to_datetime(dfc['data_base'])==database)].sum()
        return value

    # manipulação do saldo final
    def func_saldo_final(dfs, dfc, dfr, mes):
        if mes==1:
            value = func_saldo_inicial(dfs, f"2025-01-01")+func_recebido(dfr, mes)-func_custo(dfc, mes)
        else:
            value = (func_saldo_inicial(dfs, f"2025-01-01")+func_recebido(dfr, mes-1)-func_custo(dfc, mes-1))+func_recebido(dfr, mes)-func_custo(dfc, mes)
        return value

    def calculate_saldo(dfs, dfc, dfr, mes):
        database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")
        if mes==1:
            print('1')
            value =  func_saldo_inicial(dfs, database)
        else:
            print('2')
            value = (func_saldo_inicial(dfs, f"2025-01-01")+func_recebido(dfr, mes-1)-func_custo(dfc, mes-1))#+func_recebido(dfr, mes)-func_custo(dfc, mes)
        return value


    list_mes = [dt.date.strftime(pd.Timestamp(f"2025-0{mes}-01"),"%B") if mes<=9 else dt.date.strftime(pd.Timestamp(f"2025-{mes}-01"),"%B") for mes in range(1,3)]

    data_json_1={
        'saldo_inicial': [calculate_saldo(dfs, dfc, dfr, mes) if mes==1 else calculate_saldo(dfs, dfc, dfr, mes) for mes in range(1,3)],
        'entrada':[ func_recebido(dfr, mes) for mes in range(1,3)],
        'saida':[ func_custo(dfc, mes)*-1 for mes in range(1,3) ],
        'saldo_final':[func_saldo_final(dfs, dfc, dfr, mes) for mes in range(1,3)],
        'mes_base':[dt.date.strftime(pd.Timestamp(f"2025-0{mes}-01"),"%B") if mes<=9 else dt.date.strftime(pd.Timestamp(f"2025-{mes}-01"),"%B") for mes in range(1,3)]
    }
    print(data_json_1)

    # resultado_202412=(msg_negativa if ( calc_financeiro_202412 < 0 and valor_saldo_202412 < 0 ) else msg)
    df1_ = pd.DataFrame()
    df1_ = pd.DataFrame({
        'analise':['' for mes in range(1,3)],
        'saldo_inicial': [calculate_saldo(dfs, dfc, dfr, mes) if mes==1 else calculate_saldo(dfs, dfc, dfr, mes) for mes in range(1,3)],
        'entrada':[ func_recebido(dfr, mes) for mes in range(1,3)],
        'saida':[ func_custo(dfc, mes)*-1 for mes in range(1,3) ],
        'saldo_final':[func_saldo_final(dfs, dfc, dfr, mes) for mes in range(1,3)],
        'mes_base':[dt.date.strftime(pd.Timestamp(f"2025-0{mes}-01"),"%B") if mes<=9 else dt.date.strftime(pd.Timestamp(f"2025-{mes}-01"),"%B") for mes in range(1,3)]
    })
    
    return df1_

    #-R$ 1.111,60	R$ 30.000,00	-R$ 5.527,41	R$ 23.360,99	R$ 23.360,99	October


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