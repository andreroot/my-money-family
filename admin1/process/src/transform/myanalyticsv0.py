

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