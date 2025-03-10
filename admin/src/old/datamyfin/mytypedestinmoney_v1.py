def my_destin_pushout_df(df):
    
    import pandas as pd
    

    #grupos 
    df["tipo_custo"] = df.apply(lambda x: x["tipo_custo"] if x["tipo_custo"]!=None and x["tipo_custo"]!="compras" else 
                                ( "pessoa" if x["descricao"].find('Bruno Podero')>=0 or x["descricao"].find('Roberto')>=0 else
                                ( "alimentacao" if x["descricao"].find('RESTAURANTE')>=0  or x["descricao"].find('Paleteria')>=0 or x["descricao"].find('Matheus')>=0 else
                                ( "banco" if x["descricao"].find('TAR PACOTE ITAU')>=0  or x["descricao"].find('IOF')>=0 or x["descricao"].find('iof')>=0 or x["descricao"].find('Iof')>=0 or x["descricao"].find('Iof Parcelamento')>=0 or  x["descricao"].find('Juros De Mora')>=0 or x["descricao"].find('Juros De Mora')>=0 else 
                                ( "americanas" if x["descricao"].find('Ame*americanas')>=0 or  x["descricao"].find('Lojas Americ')>=0  else
                                ( "gasolina" if x["descricao"].find('Auto Posto')>=0 or x["descricao"].find('Posto Estacao')>=0 or x["descricao"].find('Posto Pe De Boi')>=0  else
                                ( "farmacia" if x["descricao"].find('Drogaria')>=0 or x["descricao"].find('Extrafarma')>=0 or x["descricao"].find('Raia')>=0 else 
                                ( "roupa" if x["descricao"].find('Centauro')>=0 or x["descricao"].find('Outlet')>=0  or x["descricao"].find('Pernambucanas')>=0  or x["descricao"].find('Cea')>=0 or x["descricao"].find('Decathlon')>=0 or x["descricao"].find('Hurley')>=0 or x["descricao"].find('Khelf')>=0 else
                                ( "comercio" if x["descricao"].find('Centro')>=0 or x["descricao"].find('Fernandinhos')>=0 or x["descricao"].find('Brands')>=0 or  x["descricao"].find('Y Model')>=0 or x["descricao"].find('ORION')>=0 else
                                ( "sorvete" if x["descricao"].find('Chiquinho')>=0 or x["descricao"].find('Pag*oggisorvetes')>=0 or x["descricao"].find('Oggi')>=0  else                                
                                ( "cabelereiro" if x["descricao"].find('erlonds')>=0 else 
                                ( "alimentacao-churros" if (x["descricao"].find('EDMILSO')>=0 or x["descricao"].find('edmilso')>=0 or x["descricao"].find('Erinald')>=0 ) else 
                                ( "saque" if x["descricao"].find('SAQUE')>=0 else
                                ( "financiamento" if x["descricao"].find('IMOBILIARIO')>=0 else
                                ( "app-amazon" if x["descricao"].find('Amazon Prime Canais')>=0 or  x["descricao"].find('Amazonprimebr')>=0  else
                                ( "plataforma-aws" if x["descricao"].find('Amazon Aws Servicos Bra')>=0  else
                                ( "arena" if x["descricao"].find('Arena')>=0 else
                                ( "creche" if x["descricao"].find('APM')>=0 else
                                ( "mala_viagem" if x["descricao"].find('Bagaggio')>=0 else
                                ( "calcados" if x["descricao"].find('Calcados')>=0 else                            
                                ( "mercado" if x["descricao"].find('Carrefour')>=0 else
                                ( "passeio_viagem" if x["descricao"].find('Crisval')>=0 else
                                ( "app-google" if x["descricao"].find('Dl*google Google')>=0 else
                                ( "plataforma-gcp" if x["descricao"].find('Dl*google Cloud')>=0 or x["descricao"].find('Google Cloud')>=0  else
                                ( "app-spotify" if x["descricao"].find('Dm*spotify')>=0 else
                                ( "locadora-localiza" if x["descricao"].find('Localiza')>=0 else
                                ( "locadora-unidas" if x["descricao"].find('Unidas')>=0 else
                                ( "catho" if x["descricao"].find('Catho')>=0 else
                                ( "kalunga" if x["descricao"].find('Kalunga')>=0 else  
                                ( "boticario" if x["descricao"].find('Hna*oboticar')>=0 else  
                                ( "loja_material" if x["descricao"].find('Lojao Da Vil')>=0 else  
                                ( "colchao" if x["descricao"].find('King Star Colchoes')>=0 else 
                                ( "parcelamento" if x["descricao"].find('Parcelamen')>=0 else 
                                ( "hotel" if x["descricao"].find('Zagaia')>=0 else
                                ( "Vent Vert" if x["descricao"].find('Vent Vert')>=0 else
                                ( "padaria" if x["descricao"].find('Padaria Deli')>=0 or x["descricao"].find('PAES E DOCE')>=0   else
                                ( "troca_oleo" if x["descricao"].find('Rei Do Oleo')>=0 else
                                ( "shopping" if x["descricao"].find('Shopping')>=0 else
                                ( "seguro" if x["descricao"].find('Tokio Marine')>=0 else
                                ( "moda" if x["descricao"].find('Max Fama Modeling')>=0 else
                                ( "ingresso" if x["descricao"].find('*sympl')>=0 else 
                                ( "mecanico" if x["descricao"].find('PIX TRANSF FGR COM')>=0 else  
                                ( "conectar" if x["descricao"].find('Conectcar')>=0 else   
                                ("igreja" if x["descricao"].find('IMW 3')>=0 else    
                                ("porto_seguro" if x["descricao"].find('PORTOSEG')>=0 else
                                ("cea" if x["descricao"].find('PIX QRS CEA')>=0 else                                
                                ("ipva" if x["descricao"].find('BKI IPVA-SPHOH6396')>=0 or x["descricao"].find('INT IPVA-')>=0	 else   
                                ("investimento" if x["descricao"].find('SAFETYPAY')>=0 or x["descricao"].find('Pagsmile')>=0 or x["descricao"].find('PAGSMILE')>=0 or x["descricao"].find('Localpay')>=0  else                                                                                              
                                None)))))))))))))))))))))))))))))))))))))))))))))))
                                ,axis=1)
                                # ( "escola_futebol" if  x["descricao"].find('Chute Inicial')>=0 or x["descricao"].find('R9 Parque')>=0 or x["descricao"].find('Real Brasil')>=0 or  x["descricao"].find('REAL BRASIL')>=0 or x["descricao"].find('PIX TRANSF  REAL')>=0 or x["descricao"].find('NIRLEY')>=0 else
                                # ( "futebol" if x["descricao"].find('Joadson')>=0 else
                                # ("emprestimo" if x["descricao"].find('PIX TRANSF  ELIZAB')>=0 else
                                # ("transporte_pub" if x["descricao"].find('PIX TRANSF  EMBRYO')>=0 else    
    return df

def my_destin_pushout_csv(df, path_csv):
    
    import pandas as pd
    import os

    print("Etapa INTERMEDIARIA DO PUSHOUT para classificação do tipo de custo","\n")


    def func(custo):

        df_type = pd.read_csv(path_csv, sep=';', usecols=['de_para','valor'], encoding='latin-1')
        #window incluir ecoding
        # encoding='latin-1'
        # configurar ';', e incluir aspas duplas '"', e tipo iso-8859-1
        df_type = df_type[(df_type["valor"].notnull())].drop_duplicates()#.unique() | .notnull() | .isnull()
        df_type = df_type[df_type['valor']!="compras"]#.unique() | .notnull() | .isnull()

        for idx, row in df_type.iterrows():
            result = None
            if custo==row['de_para']:
                
                result = row['valor']
                return result

    df["tipo_custo"] = df.apply(lambda x: func(x["descricao"]), axis=1)

    if len(df[(df["tipo_custo"].isnull())]) > 0:
        df = my_destin_pushout_df(df)

    # df_analytics = df['tipo_custo'].loc[df['tipo_custo'].isnull()]
    # #df[df['tipo_custo']==None].groupby(['dt_base']).agg(['count'])
    # total_tipo_nok=df_analytics.count()

    # total_tipo_generico=df['tipo_custo'].loc[df['tipo_custo']=="compras"].count()
    # total_tipo_classif=df['tipo_custo'].loc[df['tipo_custo'].notnull()].count()
    print(f'Total tipos_encontrados:{len(df[(df["tipo_custo"].notnull())])} / tipos_nao_encontrados:{len(df[(df["tipo_custo"].isnull())])}\n')
    #df.groupby(['table_id']).agg({'custo_dia':'sum','custo_dia_mes':'sum','table_id':'count', 'qtde_execucao_dia':'sum'})

    return df