def my_destin_pushout_csv(df, path_csv):
    
    import pandas as pd
    import os

    print("Etapa INTERMEDIARIA DO PUSHOUT para classificação do tipo de custo","\n")

    df_type = pd.read_csv(path_csv, sep=';', usecols=['de_para','valor'], encoding='latin-1')
    #window incluir ecoding
    # encoding='latin-1'
    # configurar ';', e incluir aspas duplas '"', e tipo iso-8859-1
    df_type = df_type[(df_type["valor"].notnull())].drop_duplicates()#.unique() | .notnull() | .isnull()

    for index_cc, row_cc in df.iterrows():
            
        idx = index_cc

        if len(df_type.index)>0:

            #print ("inicio da verificação de de-para custo")

            for index_dp, row_dp in df_type.iterrows():
                
                if row_cc["descricao"] == row_dp['de_para']:
                    
                    #print ("primeira verificação de de-para custo:"," / posicao:",index_dp, row_dp['de_para'], '=', row_cc["descricao"]," / posicao:",index_cc)
                    df.at[idx,"tipo_custo"] = row_dp['valor'] 

    # for index_cc, row_cc in df.iterrows():
    #     idx = index_cc

                else:

                    if row_cc["tipo_custo"]==None: 

                        #print ("segunda verificação de de-para custo:", row_cc["descricao"]," / posicao:",index_cc)

                        if row_cc["descricao"].find('SABESP')>=0:
                            tipo_custo_x = "sabesp"
                            df.at[idx,"tipo_custo"] = tipo_custo_x 
                        elif row_cc["descricao"].find('IMOBILIARIO')>=0:
                            tipo_custo_x = "financiamento"
                            df.at[idx,"tipo_custo"] = tipo_custo_x                   
                        elif row_cc["descricao"].find('REI DO OLEO')>=0 or row_cc["descricao"].find('Rei Do Oleo')>=0 or row_cc["descricao"].find('shell')>=0 or row_cc["descricao"].find('Olibone')>=0:
                            tipo_custo_x = "carro"
                            df.at[idx,"tipo_custo"] = tipo_custo_x   
                        elif row_cc["descricao"].find('TAR PACOTE ITAU')>=0:
                            tipo_custo_x = "banco"
                            df.at[idx,"tipo_custo"] = tipo_custo_x   
                        elif row_cc["descricao"].find('Mayara')>=0 or row_cc["descricao"].find('PIX TRANSF  ROGERIO')>=0:
                            tipo_custo_x = "pipoca"
                            df.at[idx,"tipo_custo"] = tipo_custo_x           
                        elif row_cc["descricao"].find('EDMILSO')>=0:
                            tipo_custo_x = "churros"
                            df.at[idx,"tipo_custo"] = tipo_custo_x
                        elif row_cc["descricao"].find('erlonds')>=0:
                            tipo_custo_x = "cabeleireiro"
                            df.at[idx,"tipo_custo"] = tipo_custo_x                                                                                                        
                        elif row_cc["descricao"].find('PIX QRS PORTOSEG')>=0:
                            tipo_custo_x = "portoseguro"
                            df.at[idx,"tipo_custo"] = tipo_custo_x  
                        elif row_cc["descricao"].find('PIX TRANSF  ROGERIO')>=0:
                            tipo_custo_x = "pipoca"
                            df.at[idx,"tipo_custo"] = tipo_custo_x                             
                        elif row_cc["descricao"].find('IMW')>=0:
                            tipo_custo_x = "igreja"
                            df.at[idx,"tipo_custo"] = tipo_custo_x 
                        elif row_cc["descricao"].find('SAQUE')>=0:
                            tipo_custo_x = "saque"
                            df.at[idx,"tipo_custo"] = tipo_custo_x 

                        # ESPECIFICO PARA CREDITO                 
                        elif row_cc["descricao"].find('olibone')>=0:
                            tipo_custo_x = "olibone"
                            df.at[idx,"tipo_custo"] = tipo_custo_x  
                        elif row_cc["descricao"].find('Oleo')>=0:
                            tipo_custo_x = "oleo"
                            df.at[idx,"tipo_custo"] = tipo_custo_x                  
                        elif row_cc["descricao"].find('americanas')>=0 or row_cc["descricao"].find('Lojas Americ')>=0:
                            tipo_custo_x = "americanas"
                            df.at[idx,"tipo_custo"] = tipo_custo_x
                        elif row_cc["descricao"].find('Cea')>=0:
                            tipo_custo_x = "cea"
                            df.at[idx,"tipo_custo"] = tipo_custo_x    
                        elif row_cc["descricao"].find('Torra')>=0:
                            tipo_custo_x = "torra"
                            df.at[idx,"tipo_custo"] = tipo_custo_x         
                        elif row_cc["descricao"].find('Parcelamen')>=0:
                            tipo_custo_x = "fautura_parcelado"
                            df.at[idx,"tipo_custo"] = tipo_custo_x 
                        elif row_cc["descricao"].find('Avila')>=0:
                            tipo_custo_x = "mercado"
                            df.at[idx,"tipo_custo"] = tipo_custo_x 
                        elif row_cc["descricao"].find('Zagaia')>=0:
                            tipo_custo_x = "viagem_hotel"
                            df.at[idx,"tipo_custo"] = tipo_custo_x   
                        elif row_cc["descricao"].find('google')>=0:
                            tipo_custo_x = "google"
                            df.at[idx,"tipo_custo"] = tipo_custo_x    
                        elif row_cc["descricao"].find('google')>=0:
                            tipo_custo_x = "google"
                            df.at[idx,"tipo_custo"] = tipo_custo_x   
                        elif row_cc["descricao"].find('Localiza')>=0 or row_cc["descricao"].find('Aluguel')>=0:
                            tipo_custo_x = "localiza"
                            df.at[idx,"tipo_custo"] = tipo_custo_x  
                        elif row_cc["descricao"].find('Padaria')>=0:
                            tipo_custo_x = "padaria"
                            df.at[idx,"tipo_custo"] = tipo_custo_x         
                        elif row_cc["descricao"].find('Amazon')>=0:
                            tipo_custo_x = "amazon"
                            df.at[idx,"tipo_custo"] = tipo_custo_x  
                        elif row_cc["descricao"].find('Oggi')>=0:
                            tipo_custo_x = "sorvete"
                            df.at[idx,"tipo_custo"] = tipo_custo_x   
                        elif row_cc["descricao"].find('Legado')>=0:
                            tipo_custo_x = "alimentação"
                            df.at[idx,"tipo_custo"] = tipo_custo_x                                                                                                                                                                                                                     
                        elif row_cc["descricao"].find('R9 Parque Nov')>=0:
                            tipo_custo_x = "futebol"
                            df.at[idx,"tipo_custo"] = tipo_custo_x 
                        elif row_cc["descricao"].find('Max Fama')>=0:
                            tipo_custo_x = "curso_modelo"
                            df.at[idx,"tipo_custo"] = tipo_custo_x  
                        elif row_cc["descricao"].find('Netflix')>=0:
                            tipo_custo_x = "netflix"
                            df.at[idx,"tipo_custo"] = tipo_custo_x     
                        elif row_cc["descricao"].find('Centauro')>=0:
                            tipo_custo_x = "centauro"
                            df.at[idx,"tipo_custo"] = tipo_custo_x   
                        elif row_cc["descricao"].find('Calcados')>=0:
                            tipo_custo_x = "calçados"
                            df.at[idx,"tipo_custo"] = tipo_custo_x                 
                        elif row_cc["descricao"].find('Chute Inicial Cori')>=0:
                            tipo_custo_x = "futebol"
                            df.at[idx,"tipo_custo"] = tipo_custo_x  
                        elif row_cc["descricao"].find('Crisval Agencia')>=0:
                            tipo_custo_x = "viagem"
                            df.at[idx,"tipo_custo"] = tipo_custo_x    
                        elif row_cc["descricao"].find('Khelf')>=0:
                            tipo_custo_x = "khelf"
                            df.at[idx,"tipo_custo"] = tipo_custo_x 
                        elif row_cc["descricao"].find('Fernandinhos')>=0:
                            tipo_custo_x = "roupas"
                            df.at[idx,"tipo_custo"] = tipo_custo_x 
                        elif row_cc["descricao"].find('Jarouche')>=0:
                            tipo_custo_x = "roupas"
                            df.at[idx,"tipo_custo"] = tipo_custo_x                 
                        elif row_cc["descricao"].find('Artemis')>=0:
                            tipo_custo_x = "roupas"
                            df.at[idx,"tipo_custo"] = tipo_custo_x  
                        elif row_cc["descricao"].find('Bagaggio')>=0:
                            tipo_custo_x = "mala"
                            df.at[idx,"tipo_custo"] = tipo_custo_x 
                        elif row_cc["descricao"].find('Pernambucanas')>=0:
                            tipo_custo_x = "pernambucanas"
                            df.at[idx,"tipo_custo"] = tipo_custo_x         
                        elif row_cc["descricao"].find('Hurley')>=0:
                            tipo_custo_x = "hurley"
                            df.at[idx,"tipo_custo"] = tipo_custo_x
                        elif row_cc["descricao"].find('Extrafarma')>=0:
                            tipo_custo_x = "farmacia"
                            df.at[idx,"tipo_custo"] = tipo_custo_x    
                        elif row_cc["descricao"].find('boticar')>=0:
                            tipo_custo_x = "boticario"
                            df.at[idx,"tipo_custo"] = tipo_custo_x
                        elif row_cc["descricao"].find('King Star')>=0:
                            tipo_custo_x = "king star"
                            df.at[idx,"tipo_custo"] = tipo_custo_x                
                        elif row_cc["descricao"].find('Tokio Marine')>=0:
                            tipo_custo_x = "tokiomarine"
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