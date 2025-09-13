
import pandas as pd   
import datetime as dt
import re
from datetime import timedelta, date

def calculate_prevision(data_json, ind, columns):
    # def func(ind,columns):
        # data_json = df[df['mes_base']==mes].to_dict(orient="records")
    return data_json[columns][ind]

def calculate_prevanalytics(dfs, dfc, dfr):

    # manipulação do saldo inicial
    def func_saldo_inicial(dfs, mes):
        database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")
        min_data_saldo = dfs['dt_receb'][(dfs['valor_saldo']!=1.00) & ((pd.to_datetime(dfs['data_base'])==database)) ].min()
        valor_saldo_ = dfs['valor_saldo'][(pd.to_datetime(dfs['data_base'])==database) & (pd.to_datetime(dfs['dt_receb'])==min_data_saldo)].max()
        return valor_saldo_ 

    def calculate_saldo(dfs, dfc, dfr, mes):
        def func_saldo_retorativo(mes):
            value_2 = func_saldo_inicial(dfs, mes)+func_recebido(dfr, mes)-func_custo(dfc, mes)
            return value_2
        
        if mes==1:
            value =  func_saldo_inicial(dfs, mes)
        
        # PREVISAO DE SALDO
        elif mes>dt.datetime.now().month:
            value_m0 = func_saldo_retorativo(dt.datetime.now().month)
            # print(value_m0)
            # value_m1 = func_saldo_retorativo(dt.datetime.now().month+1)
            # print(value_m1)
            # value_m2 = func_saldo_retorativo(dt.datetime.now().month+2)
            # print(value_m2)

            if mes == 11:
                # somar com 
                value = value_m0+(func_saldo_inicial(dfs, 10)+func_recebido(dfr, 10)-func_custo(dfc, 10))
            elif mes == 12:
                value = value_m0+(func_saldo_inicial(dfs, 10)+func_recebido(dfr, 10)-func_custo(dfc, 10))+(func_saldo_inicial(dfs, 11)+func_recebido(dfr, 11)-func_custo(dfc, 11))
            else:
                value = (func_saldo_inicial(dfs, mes-1)+func_recebido(dfr, mes-1)-func_custo(dfc, mes-1))
            # print(f"DEBUG: SALDO INICIAL called for date {mes}, returning {value} | {value_2}\n")
        else:
            value = (func_saldo_inicial(dfs, mes-1)+func_recebido(dfr, mes-1)-func_custo(dfc, mes-1))#+func_recebido(dfr, mes)-func_custo(dfc, mes)
        # print(f"DEBUG: SALDO INICIAL called for date {mes}, returning {value} | {value_2}\n")
        return value

    # manipulação do recebido
    def func_recebido(dfr, mes):
        database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")
        # MES DO RECEBIMENTO DA RECISÃO DE CONTRATO
        if mes==10:
            value = dfr['valor_receb'].loc[(pd.to_datetime(dfr['data_base'])==database)].sum()+40000
        else:
            value = dfr['valor_receb'].loc[(pd.to_datetime(dfr['data_base'])==database)].sum()
        
        # print(f"DEBUG: func_recebido called for date {database}, returning {value}")
        return value

    # manipulação do custos
    def func_custo(dfc, mes):
        database = pd.Timestamp(f"2025-0{mes}-01") if mes<=9 else pd.Timestamp(f"2025-{mes}-01")

        if mes>=dt.datetime.now().month:
            # Para os meses de fake adiciona 3000 ao custo
            # Isso é uma regra de negócio específica, talvez para cobrir custos extras nesses meses
            value = dfc['valor_custo'].loc[(pd.to_datetime(dfc['data_base'])==database)].sum()+3000
        else:
            value = dfc['valor_custo'].loc[(pd.to_datetime(dfc['data_base'])==database)].sum()

        # print(f"DEBUG: func_custo called for date {database}, returning {value}")
        return value

    # manipulação do saldo final
    def func_saldo_final(dfs, dfc, dfr, mes):
        if mes==1:
            value = calculate_saldo(dfs, dfc, dfr, mes)+func_recebido(dfr, mes)-func_custo(dfc, mes)
            value_2 =  'None'
        elif mes>dt.datetime.now().month:
            value_2 = calculate_saldo(dfs, dfc, dfr, dt.datetime.now().month)+func_recebido(dfr, dt.datetime.now().month)-func_custo(dfc, dt.datetime.now().month)
            
            value = calculate_saldo(dfs, dfc, dfr, mes)+func_recebido(dfr, mes)-func_custo(dfc, mes)
            print(f"DEBUG: SALDO FINAL called for date {mes}, returning {value} | {value_2}\n")
        else:
            value = calculate_saldo(dfs, dfc, dfr, mes)+func_recebido(dfr, mes)-func_custo(dfc, mes)
            value_2 = value+func_recebido(dfr, mes)-func_custo(dfc, mes)
        
        return value

    # list_mes = [dt.date.strftime(pd.Timestamp(f"2025-0{mes}-01"),"%B") if mes<=9 else dt.date.strftime(pd.Timestamp(f"2025-{mes}-01"),"%B") for mes in range(1,3)]

    data_json_1={
        'saldo_inicial': [calculate_saldo(dfs, dfc, dfr, mes) for mes in range(1,13)],
        # 'saldo_inicial_prev': [calculate_saldo(dfs, dfc, dfr, mes) for mes in range(1,13)],
        'entrada':[ func_recebido(dfr, mes) for mes in range(1,13)],
        'saida':[ func_custo(dfc, mes)*-1 for mes in range(1,13) ],
        'saldo_final':[func_saldo_final(dfs, dfc, dfr, mes) for mes in range(1,13)],
        # 'saldo_final_prev':[func_saldo_final(dfs, dfc, dfr, mes) for mes in range(1,13)],
        'mes_base':[dt.date.strftime(pd.Timestamp(f"2025-0{mes}-01"),"%B") if mes<=9 else dt.date.strftime(pd.Timestamp(f"2025-{mes}-01"),"%B") for mes in range(1,13)]
    }

    return data_json_1

def calculate_analytics(dfs, dfc, dfr):

    # manipulação do saldo inicial
    data_json = calculate_prevanalytics(dfs, dfc, dfr)

    # manipulação do saldo inicial
    def func_saldo_inicial(dfs, database):
        value = data_json['saldo_final'][0]
        return value

    print(data_json['saldo_inicial'][0])


    return 