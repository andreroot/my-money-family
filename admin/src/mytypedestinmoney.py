def my_destin_pushout_csv(df, path_csv):
    import pandas as pd
    import os

    print("Etapa INTERMEDIARIA DO PUSHOUT para classificação do tipo de custo","\n")

    df_type = pd.read_csv(path_csv, sep=';', usecols=['de_para','valor'])
    df_type = df_type[(df_type["valor"].notnull())].drop_duplicates()#.unique() | .notnull() | .isnull()

    for index_cc, row_cc in df.iterrows():
            
        idx = index_cc

        if len(df_type.index)>0:

            #print ("inicio da verificação de de-para custo")

            for index_dp, row_dp in df_type.iterrows():
                
                if row_cc["descricao"] == row_dp['de_para']:
                    
                    #print ("primeira verificação de de-para custo:"," / posicao:",index_dp, row_dp['de_para'], '=', row_cc["descricao"]," / posicao:",index_cc)
                    df.at[idx,"tipo_custo"] = row_dp['valor'] 

    
    for index_cc, row_cc in df.iterrows():
        
        idx = index_cc

        if row_cc["tipo_custo"]==None: 

            #print ("segunda verificação de de-para custo:", row_cc["descricao"]," / posicao:",index_cc)

            if row_cc["descricao"].find('SABESP')>=0:
                tipo_custo_x = "sabesp"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('IMOBILIARIO')>=0:
                tipo_custo_x = "financiamento"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('B2W COMPANH')>=0 or row_cc["descricao"].find('americanas')>=0:
                tipo_custo_x = "americanas"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('APLICACAO CDB')>=0:
                tipo_custo_x = "aplicacao"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('Cea')>=0 or row_cc["descricao"].find('Torra Torra')>=0 or row_cc["descricao"].find('Brooksfield')>=0:
                tipo_custo_x = "roupa"
                df.at[idx,"tipo_custo"] = tipo_custo_x                     
            elif row_cc["descricao"].find('REI DO OLEO')>=0 or row_cc["descricao"].find('Rei Do Oleo')>=0 or row_cc["descricao"].find('shell')>=0 or row_cc["descricao"].find('Olibone')>=0:
                tipo_custo_x = "carro"
                df.at[idx,"tipo_custo"] = tipo_custo_x   
            elif row_cc["descricao"].find('TAR PACOTE ITAU')>=0:
                tipo_custo_x = "banco"
                df.at[idx,"tipo_custo"] = tipo_custo_x   
            elif row_cc["descricao"].find('Mayara')>=0:
                tipo_custo_x = "pipoca"
                df.at[idx,"tipo_custo"] = tipo_custo_x           
            elif row_cc["descricao"].find('EDMILSO')>=0:
                tipo_custo_x = "churros"
                df.at[idx,"tipo_custo"] = tipo_custo_x

            elif row_cc["descricao"].find('erlonds')>=0:
                tipo_custo_x = "cabeleireiro"
                df.at[idx,"tipo_custo"] = tipo_custo_x                                                                                                        

            elif row_cc["descricao"].find('SABESP')>=0:
                tipo_custo_x = "sabesp"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('IMOBILIARIO')>=0:
                tipo_custo_x = "financiamento"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('B2W COMPANH')>=0 or row_cc["descricao"].find('americanas')>=0:
                tipo_custo_x = "americanas"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('APLICACAO CDB')>=0:
                tipo_custo_x = "aplicacao"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('Cea')>=0 or row_cc["descricao"].find('Torra Torra')>=0 or row_cc["descricao"].find('Brooksfield')>=0:
                tipo_custo_x = "roupa"
                df.at[idx,"tipo_custo"] = tipo_custo_x                     
            elif row_cc["descricao"].find('REI DO OLEO')>=0 or row_cc["descricao"].find('Rei Do Oleo')>=0 or row_cc["descricao"].find('shell')>=0 or row_cc["descricao"].find('Olibone')>=0:
                tipo_custo_x = "carro"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('TAR PACOTE ITAU')>=0:
                tipo_custo_x = "banco"
                df.at[idx,"tipo_custo"] = tipo_custo_x   
            elif row_cc["descricao"].find('Mayara')>=0:
                tipo_custo_x = "pipoca"
                df.at[idx,"tipo_custo"] = tipo_custo_x           
            elif row_cc["descricao"].find('EDMILSO')>=0:
                tipo_custo_x = "churros"
                df.at[idx,"tipo_custo"] = tipo_custo_x     
            elif row_cc["descricao"].find('erlonds')>=0:
                tipo_custo_x = "cabeleireiro"
                df.at[idx,"tipo_custo"] = tipo_custo_x                                                       
            elif row_cc["descricao"].find('SABESP')>=0:
                tipo_custo_x = "sabesp"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('IMOBILIARIO')>=0:
                tipo_custo_x = "financiamento"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('B2W COMPANH')>=0 or row_cc["descricao"].find('americanas')>=0:
                tipo_custo_x = "americanas"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('APLICACAO CDB')>=0:
                tipo_custo_x = "aplicacao"
                df.at[idx,"tipo_custo"] = tipo_custo_x 
            elif row_cc["descricao"].find('Cea')>=0 or row_cc["descricao"].find('Torra Torra')>=0 or row_cc["descricao"].find('Brooksfield')>=0:
                tipo_custo_x = "roupa"
                df.at[idx,"tipo_custo"] = tipo_custo_x                     
            elif row_cc["descricao"].find('REI DO OLEO')>=0 or row_cc["descricao"].find('Rei Do Oleo')>=0 or row_cc["descricao"].find('shell')>=0 or row_cc["descricao"].find('Olibone')>=0:
                tipo_custo_x = "carro"
                df.at[idx,"tipo_custo"] = tipo_custo_x        
            elif row_cc["descricao"].find('TAR PACOTE ITAU')>=0:
                tipo_custo_x = "banco"
                df.at[idx,"tipo_custo"] = tipo_custo_x   
            elif row_cc["descricao"].find('Mayara')>=0:
                tipo_custo_x = "pipoca"
                df.at[idx,"tipo_custo"] = tipo_custo_x           
            elif row_cc["descricao"].find('EDMILSO')>=0:
                tipo_custo_x = "churros"
                df.at[idx,"tipo_custo"] = tipo_custo_x   
            elif row_cc["descricao"].find('erlonds')>=0:
                tipo_custo_x = "cabeleireiro"
                df.at[idx,"tipo_custo"] = tipo_custo_x                                                      

            else:
                #print ("terceira verificação de de-para custo:",row_cc["descricao"]," / posicao:",index_cc)

                tipo_custo_x = "compras"
                df.at[idx,"tipo_custo"] = tipo_custo_x 

    for x in range(len(df)):
        if df["tipo_custo"].iloc[x]==None:
            #print ("quarta verificação de de-para custo:",row_cc["descricao"]," / posicao:",index_cc)

            df["tipo_custo"].iloc[x]="compras" 
    
    df_analytics = df['tipo_custo'].loc[df['tipo_custo']==None]
    #df[df['tipo_custo']==None].groupby(['dt_base']).agg(['count'])
    total_tipo_nok=df_analytics.count()

    total_tipo_generico=df['tipo_custo'].loc[df['tipo_custo']=="compras"].count()
    total_tipo_classif=df['tipo_custo'].loc[df['tipo_custo']!="compras"].count()
    print(f'Total tipos_encontrados:{total_tipo_classif} / tipos_nao_encontrados:{total_tipo_nok} / tipo_generico:{total_tipo_generico}\n')
    #df.groupby(['table_id']).agg({'custo_dia':'sum','custo_dia_mes':'sum','table_id':'count', 'qtde_execucao_dia':'sum'})

    return df