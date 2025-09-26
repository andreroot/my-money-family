
import pandas as pd   
import pandas_gbq as pgbq 
import datetime as dt
import re
from datetime import timedelta, date

from sheets.generate_data import analytics_sheets
# ANALYTICS MENSAL PARA FIXO E VARIAVEL

def extract_analytics(df):

    #df = process(query, flag_fake=False)
    # print(df.columns)

    list_dt=df[['data_base']].sort_values("data_base", ascending=True).drop_duplicates().values.tolist()

    def get_valor_custo(df, tp, dt):
        return df[["valor_custo"]][ (df['tipo_custo']==tp)&(pd.to_datetime(df['data_base'])==dt)].sum().values.tolist()

    # LISTAGEM DO TIPO DE CUSTO
    list_tp=df[["tipo_custo"]].drop_duplicates().values.tolist()

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

        valor_sum_anual = df[["valor_custo"]][ (df['tipo_custo']==rw[0])].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_med_anual =  float(valor_sum_anual[0]/len(list_dt))
        
        # print(f"DEBUG: TIPO CUSTO: {rw[0]} - VALOR ANUAL: {valor_sum_anual} - VALOR MEDIO: {valor_med_anual} - DATA BASE {list_dt[0][0]}")

        valor_jan   = get_valor_custo(df, rw[0], list_dt[0][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False)
        valor_fev   = get_valor_custo(df, rw[0], list_dt[1][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mar   = get_valor_custo(df, rw[0], list_dt[2][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_abr   = get_valor_custo(df, rw[0], list_dt[3][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_mai   = get_valor_custo(df, rw[0], list_dt[4][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jun   = get_valor_custo(df, rw[0], list_dt[5][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_jul   = get_valor_custo(df, rw[0], list_dt[6][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_ago   = get_valor_custo(df, rw[0], list_dt[7][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_set   = get_valor_custo(df, rw[0], list_dt[8][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_out   = get_valor_custo(df, rw[0], list_dt[9][0])# dt.date.strftime(param,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_nov   = get_valor_custo(df, rw[0], list_dt[10][0])# dt.date.strftime(param,,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
        valor_dez   = get_valor_custo(df, rw[0], list_dt[11][0])# dt.date.strftime(param,,"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)

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


def extract_analytics_sheets(df):

    # manipulação do saldo inicial
    dfs = extract_analytics(df)

    analytics_sheets(dfs, sheets="resumo financeiro", tab="custo_extract_all")
    # # manipulação do saldo inicial
    # def func_saldo_inicial(dfs, database):
    #     value = data_json['saldo_final'][0]
    #     return value

    # print(data_json['saldo_inicial'][0])