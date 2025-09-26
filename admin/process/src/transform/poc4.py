

# def process(df, flag_fake=False):


#     # validacao 

#     nulos=df[(df["tipo_custo"].isnull())][['custo','valor_custo','dt_custo']].drop_duplicates().values.tolist()

#     if len(nulos):
#         print('verificar nulos:',nulos)
        
#         df=df[~(df["tipo_custo"].isnull())].copy()

#     df = construcao_tipo_custo(df)

#     # tratar gasto pessoal
#     df['tipo_custo_alt'] = df.apply(lambda x: 'churrasco-pessoal' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
#                                                                     x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-09-01' and
#                                                                     x["dt_custo"].strftime('%Y-%m-%d')=='2024-09-02' and
#                                                                     x["valor_custo"]==468.25  else x['tipo_custo_alt'], axis=1)

#     df['tipo_custo'] = df.apply(lambda x: 'churrasco-pessoal' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
#                                                                     x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-09-01' and
#                                                                     x["dt_custo"].strftime('%Y-%m-%d')=='2024-09-02' and
#                                                                     x["valor_custo"]==468.25  else x['tipo_custo'], axis=1)

#     # tratar gasto pessoal
#     df['tipo_custo_alt'] = df.apply(lambda x: 'churrasco-futebolamigos' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
#                                                                     x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-12-01' and
#                                                                     x["dt_custo"].strftime('%Y-%m-%d')=='2024-12-13' and
#                                                                     x["valor_custo"]==657.10		  else x['tipo_custo_alt'], axis=1)

#     df['tipo_custo'] = df.apply(lambda x: 'churrasco-futebolamigos' if x["custo"].find("ELECTRONATACADAO 06")>=0 and
#                                                                     x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-12-01' and
#                                                                     x["dt_custo"].strftime('%Y-%m-%d')=='2024-12-13' and
#                                                                     x["valor_custo"]==657.10	  else x['tipo_custo'], axis=1)


#     # tratar gasto pessoal
#     df['tipo_custo_alt'] = df.apply(lambda x: 'ceia-fimdeano' if (x["custo"].find("ELECTRONHIP BERGAMI")>=0 or x["custo"].find("ELECTRONATACADAO 06")>=0 or x["custo"].find("ELECTRONRS AVILA")>=0) and
#                                                                     x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-12-01' and
#                                                                     x["dt_custo"].strftime('%Y-%m-%d') in ('2024-12-23','2024-12-24','2024-12-30','2024-12-31') and
#                                                                     x["valor_custo"] in (225.03,185.81,355.66,94.61)		  else x['tipo_custo_alt'], axis=1)

#     df['tipo_custo'] = df.apply(lambda x: 'ceia-fimdeano' if (x["custo"].find("ELECTRONHIP BERGAMI")>=0 or x["custo"].find("ELECTRONATACADAO 06")>=0 or x["custo"].find("ELECTRONRS AVILA")>=0) and
#                                                                     x["dt_mes_base"].strftime('%Y-%m-%d')=='2024-12-01' and
#                                                                     x["dt_custo"].strftime('%Y-%m-%d') in ('2024-12-23','2024-12-24','2024-12-30','2024-12-31') and
#                                                                     x["valor_custo"] in (225.03,185.81,355.66,94.61)		  else x['tipo_custo'], axis=1)
    
#     df['tipo_custo_alt']=df.apply(lambda x: 'banco' if  x['custo'].find('TAR PACOTE ITAU')>=0 
#                                   else ('Taxa_banco' if x['custo'].find('IOF')>=0 
#                                         else ( 'juros' if x['custo'].find('JUROS LIMITE DA CONTA')>=0 else   x['tipo_custo_alt'])), axis=1)

#     df['tipo_custo']=df.apply(lambda x: 'banco' if  x['custo'].find('TAR PACOTE ITAU')>=0 
#                               else ('Taxa_banco' if x['custo'].find('IOF')>=0 
#                                     else ( 'juros' if x['custo'].find('JUROS LIMITE DA CONTA')>=0 else   x['tipo_custo'])), axis=1)


#     return df

# # ANALYTICS MENSAL PARA FIXO E VARIAVEL

# def analytics_percent_geral(df):

#     # df = process(query, flag_fake=False)

#     list_dt=df[['dt_mes_base']].sort_values("dt_mes_base", ascending=True).drop_duplicates().values.tolist()

#     def get_valor_custo(df, tp, dt):
#         return df[["valor_custo"]][ (df['tipo_custo_alt']==tp)&(pd.to_datetime(df['dt_mes_base'])==dt)].sum().values.tolist()

#     # LISTAGEM DO TIPO DE CUSTO
#     list_tp=df[["tipo_custo_alt"]].drop_duplicates().values.tolist()
    
#     arr_tp_ext=[]

#     valor_jan_sum = 0
#     valor_fev_sum = 0
#     valor_mar_sum = 0
#     valor_abr_sum = 0
#     valor_mai_sum = 0
#     valor_jun_sum = 0
#     valor_jul_sum = 0
#     valor_ago_sum = 0
#     valor_set_sum = 0
#     valor_out_sum = 0
#     valor_nov_sum = 0
#     valor_dez_sum = 0

#     valor_med_anual_sum = 0
#     valor_sum_anual_sum = 0
    
#     for idx, rw in enumerate(list_tp):

#         valor_sum_anual = df[["valor_custo"]][ (df['tipo_custo_alt']==rw[0])].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_med_anual =  float(valor_sum_anual[0]/len(list_dt))

#         valor_jan   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[0][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False)
#         valor_fev   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[1][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-02-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_mar   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[2][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-03-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_abr   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[3][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-04-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_mai   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[4][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-05-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_jun   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[5][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-06-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_jul   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[6][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-07-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_ago   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[7][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-08-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_set   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[8][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-09-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_out   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[9][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-10-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_nov   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[10][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-11-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)
#         valor_dez   = get_valor_custo(df, rw[0], dt.date.strftime(list_dt[11][0],"%Y-%m-%d")) #.sort_values("valor_custo", ascending=False) df[["valor_custo"]][ (df['classificacao_custo']=='fixo') & (df['tipo_custo_alt']==rw[0])&(pd.to_datetime(df['dt_mes_base'])=='2024-12-01')].sum().values.tolist() #.sort_values("valor_custo", ascending=False)

#         ar=[rw[0],valor_sum_anual[0],valor_med_anual,valor_jan[0],valor_fev[0],valor_mar[0],valor_abr[0],valor_mai[0],valor_jun[0],valor_jul[0],valor_ago[0],valor_set[0],valor_out[0],valor_nov[0], valor_dez[0]]
#         arr_tp_ext.append(ar)

#         valor_jan_sum = valor_jan_sum + valor_jan[0]
#         valor_fev_sum = valor_fev_sum + valor_fev[0]
#         valor_mar_sum = valor_mar_sum + valor_mar[0]
#         valor_abr_sum = valor_abr_sum + valor_abr[0]
#         valor_mai_sum = valor_mai_sum + valor_mai[0]
#         valor_jun_sum = valor_jun_sum + valor_jun[0]
#         valor_jul_sum = valor_jul_sum + valor_jul[0]
#         valor_ago_sum = valor_ago_sum + valor_ago[0]
#         valor_set_sum = valor_set_sum + valor_set[0]
#         valor_out_sum = valor_out_sum + valor_out[0]
#         valor_nov_sum = valor_nov_sum + valor_nov[0]
#         valor_dez_sum = valor_dez_sum + valor_dez[0]
#         valor_med_anual_sum = valor_med_anual_sum + valor_med_anual
#         valor_sum_anual_sum = valor_sum_anual_sum + valor_sum_anual[0]

#     ar=['Total', valor_sum_anual_sum, valor_med_anual_sum, valor_jan_sum, valor_fev_sum, valor_mar_sum, valor_abr_sum, valor_mai_sum, valor_jun_sum, valor_jul_sum, valor_ago_sum, valor_set_sum, valor_out_sum, valor_nov_sum, valor_dez_sum]
#     arr_tp_ext.append(ar)

#     df_tp = pd.DataFrame(arr_tp_ext, columns=['tipo','vlr_sum','vlr_med','vlr_jan','vlr_fev','vlr_mar','vlr_abr','vlr_mai','vlr_jun','vlr_jul','vlr_ago','vlr_set','vlr_out','vlr_nov','vlr_dez'])

#     return df_tp



# def analytics(df, data_base, MES):

#     total_geral = df['valor_custo'][(pd.to_datetime(df['dt_mes_base'])==data_base)].sum()
#     total_geral_fixo_al = df.loc[(df['classificacao_custo']=='fixo') & (df['area_custo']=='alto') & (pd.to_datetime(df['dt_mes_base'])==data_base)& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
#     total_geral_fixo_md = df.loc[(df['classificacao_custo']=='fixo') & (df['area_custo']=='medio') &  (pd.to_datetime(df['dt_mes_base'])==data_base)& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
#     total_geral_fixo_bx = df.loc[(df['classificacao_custo']=='fixo') & (df['area_custo']=='medio_n1') &  (pd.to_datetime(df['dt_mes_base'])==data_base)& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
#     total_geral_variado = df.loc[(df['classificacao_custo']=='variado') & (pd.to_datetime(df['dt_mes_base'])==data_base)]['valor_custo'].sum()

#     msg=f"{MES} | custo geral: {total_geral} | custo fixo(alto): {total_geral_fixo_al} | custo fixo(medio): {total_geral_fixo_md} | custo fixo(baixo): {total_geral_fixo_bx}| custo variado: {total_geral_variado}"

#     return msg


# def analytcis_mes_custo(df):

#     import datetime
#     from datetime import date, timedelta
#     from calendar import monthrange

#     mes=[]
#     #list_data=['2024-01-01','2024-01-01','2024-01-01','2024-04','2024-05', '2024-06']

#     for m in range(1,13):
#         print(m)
#         dt_mes_base = f'2024-{m}-01'

#         dt_ini=datetime.datetime.strptime(dt_mes_base, '%Y-%m-%d')+timedelta(days=0)
#         print(dt_ini)
#         dt_fin=dt_ini.replace(day=monthrange(dt_ini.year, dt_ini.month)[1])+timedelta(days=0)
#         print(dt_fin)

#         t = df.loc[(pd.to_datetime(df['dt_mes_base'])==dt_ini.strftime('%Y-%m-%d')) \
#                 & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
        
#         f = df.loc[(df['classificacao_custo']=='fixo') & (pd.to_datetime(df['dt_mes_base'])==dt_ini.strftime('%Y-%m-%d')) \
#                 & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
        
#         v = df.loc[(df['classificacao_custo']=='variado') & (pd.to_datetime(df['dt_mes_base'])==dt_ini.strftime('%Y-%m-%d')) \
#                     & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))]['valor_custo'].sum()
        

#         # dt_mes_base_ant=(dt_ini+timedelta(days=-1)).replace(day=1)
#         # dt_ini_ant=dt_mes_base_ant+timedelta(days=0)
#         # print(dt_ini_ant)
#         # dt_fin_ant=dt_ini_ant.replace(day=monthrange(dt_ini_ant.year, dt_ini_ant.month)[1])+timedelta(days=0)
#         # print(dt_fin_ant)

#         # v_mes_ant = df.loc[(df['classificacao_custo']=='variado') & (pd.to_datetime(df['dt_mes_base'])==dt_ini_ant.strftime('%Y-%m-%d'))\
#         #                 & ((pd.to_datetime(df['dt_custo'])>=dt_ini_ant)&(pd.to_datetime(df['dt_custo'])<=dt_fin_ant))]['valor_custo'].sum()

#         # if v_mes_ant>0:
#         #     perc = (v*100)/v_mes_ant
#         # else:
#         #     perc =0
#         if t>0:
#             perc_f=round(float((f/t)*100),2)
#             perc_v=round(float((v/t)*100),2)
#         else:
#             perc_f=0
#             perc_v=0

#         ar=[dt_ini.strftime('%Y-%m'),dt_ini,t,f, perc_f,v, perc_v, dt_fin]
#         mes.append(ar)

#     return mes


# def analytcis_mes_custo_periodo(df, dias_1, dias_2):
 
#     import datetime
#     from datetime import date, timedelta
#     from calendar import monthrange

#     mes=[]
#     #list_data=['2024-01-01','2024-01-01','2024-01-01','2024-04','2024-05', '2024-06']

#     for m in range(1,13):
#         #print(m)
#         dt_mes_base = f'2024-{m}-01'

#         dt_ini=datetime.datetime.strptime(dt_mes_base, '%Y-%m-%d')+timedelta(days=dias_1)
#         #print(dt_ini)
#         dt_fin=dt_ini.replace(day=monthrange(dt_ini.year, dt_ini.month)[1])+timedelta(days=dias_2)
#         #print(dt_fin)

#         t = df.loc[((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))]['valor_custo'].sum()
        
#         f = df.loc[(df['classificacao_custo']=='fixo')\
#                 & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))& (~df['tipo_custo_alt'].isin(['poupanca']))]['valor_custo'].sum()
        
#         v = df.loc[(df['classificacao_custo']=='variado')\
#                     & ((pd.to_datetime(df['dt_custo'])>=dt_ini)&(pd.to_datetime(df['dt_custo'])<=dt_fin))]['valor_custo'].sum()
        


#         # dt_mes_base_ant=(dt_ini+timedelta(days=-1)).replace(day=1)
#         # dt_ini_ant=dt_mes_base_ant+timedelta(days=dias_1)
#         # print(dt_ini_ant)
#         # dt_fin_ant=dt_ini_ant.replace(day=monthrange(dt_ini_ant.year, dt_ini_ant.month)[1])+timedelta(days=dias_2)
#         # print(dt_fin_ant)

#         # v_mes_ant = df.loc[(df['classificacao_custo']=='variado') & (pd.to_datetime(df['dt_mes_base'])==dt_ini_ant.strftime('%Y-%m-%d'))\
#         #                 & ((pd.to_datetime(df['dt_custo'])>=dt_ini_ant)&(pd.to_datetime(df['dt_custo'])<=dt_fin_ant))]['valor_custo'].sum()

#         # if v_mes_ant>0:
#         #     perc = (v/v_mes_ant)*100
#         # else:
#         #     perc =0
#         if t>0:
#             perc_f=float((f/t)*100)
#             perc_v=float((v/t)*100)
#         else:
#             perc_f=0
#             perc_v=0

#         ar=[dt_ini.strftime('%Y-%m'),dt_ini,t,f, perc_f,v, perc_v, dt_fin]
#         mes.append(ar)

#         # ar=[dt_ini.strftime('%Y-%m'),dt_ini,t,f,v,dt_fin, dt_mes_base_ant, v_mes_ant, perc]
#         # mes.append(ar)

#     return mes
