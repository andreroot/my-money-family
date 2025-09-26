

import pandas as pd   
import datetime as dt
import re
from datetime import timedelta, date
from transform.mycalculate import calculate_prevanalytics, calculate_prevision

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

def calculate_analytics(dfs, dfc, dfr):

    # manipulação do saldo inicial
    data_json = calculate_prevanalytics(dfs, dfc, dfr)
    
    # resultado_202412=(msg_negativa if ( calc_financeiro_202412 < 0 and valor_saldo_202412 < 0 ) else msg)
    df_ = pd.DataFrame()
    df_ = pd.DataFrame({
        'analise':['' for mes in range(len(data_json['mes_base']))],
        'saldo_inicial': [data_json['saldo_inicial'][mes] for mes in range(len(data_json['mes_base']))],
        'entrada':[data_json['entrada'][mes] for mes in range(len(data_json['mes_base']))],
        'saida':[ data_json['saida'][mes] for mes in range(len(data_json['mes_base'])) ],
        'saldo_final':[ data_json['saldo_final'][mes] for mes in range(len(data_json['mes_base'])) ],
        'prev_saldo_final':[ data_json['saldo_final'][mes] for mes in range(len(data_json['mes_base']))  ],
        'mes_base':[ data_json['mes_base'][mes] for mes in range(len(data_json['mes_base']))]
    })
    
    # df__ = calculate_prevision(df_)

    # # df_ = pd.merge(df_, df__, how='left', left_on=('mes_base'), right_on='mes_base')
    # df_final = pd.concat([df_.iloc[:-3], df__], ignore_index=True)


    df_['analise'] = df_.apply(lambda x: func_analise(x['saldo_inicial'], x['entrada'], x['prev_saldo_final'], x['mes_base']), axis=1)

    return df_

