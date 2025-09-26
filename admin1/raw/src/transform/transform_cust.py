import pandas as pd
import os
import datetime as dt  
import re  

# sheets
import gspread
from gspread_dataframe import get_as_dataframe

#
# conceito de push: empurrar algo, são os gastos empurrados para fora do caixa financeiro para algum destino, são o destino final do fluxo
#
def connect():
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/andre/.ssh/my-chave-gcp-devsamelo2.json'
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/app/.ssh/my-chave-gcp-devsamelo2.json'
    gc = gspread.service_account(filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    return gc

def func_generate_sheets_type_cust(df):

    # print("DEFINIÇÃO DE TIPO DE CUSTO","\n")


    # Abra a planilha pelo nome
    sh = connect().open("depara_tipo_custo")
    worksheet = sh.worksheet("depara")

    # Lê os dados como DataFrame
    dftype = get_as_dataframe(worksheet, evaluate_formulas=True)
    #
    dftype = dftype[(dftype["valor"].notnull())].drop_duplicates()#.unique() | .notnull() | .isnull()
    dftype = dftype[dftype['valor']!="compras"]#.unique() | .notnull() | .isnull()

    list_tp = dftype[['de_para','valor']].values.tolist()

    df["tipo_custo"] = df.apply(lambda x: next((y[1] for y in list_tp if y[0] in x["descricao"]), None), axis=1)

    return df

def func_generate_calender(df):

    # print("DEFINIÇÃO DE CALENDAR","\n")


    df['week_br'] = pd.to_datetime(df["dt_custo"]).dt.day_name()
    df['week_br'] = df.apply(lambda x: "segunda-feira" if x["week_br"]=="Monday" else 
                                ("terça-feira" if x["week_br"]=="Tuesday" else 
                                ("quarta-feira" if x["week_br"]=="Wednesday" else 
                                ("quinta-feira" if x["week_br"]=="Thursday" else 
                                ("sexta-feira" if x["week_br"]=="Friday" else 
                                ("sabado" if x["week_br"]=="Saturday" else "domingo"))))), axis=1)

    return df

def func_generate_depto_cust(df):

    # print("DEFINIÇÃO DE DEPARTAMENTO DE CUSTO","\n")


    df["dept_custo"]=df.apply(lambda x: ''.join(re.findall('^\w+',x["tipo_custo"])) if x["tipo_custo"]!=None else None, axis=1)
  
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


    df["classificacao_custo"] = df.apply(lambda x: 'fixo' if [tp for tp in lista_tp if tp==x["dept_custo"]] else 'variado', axis=1)

    df["area_custo"] = df.apply(lambda x: 'alto' if [tp for tp in list_tp_alto if tp==x["dept_custo"] ] else 
                                ('medio' if [tp for tp in list_tp_medio if tp==x["dept_custo"]] else 
                                ('medio_n1' if [tp for tp in list_tp_medio_n1 if tp==x["dept_custo"]] else 
                                    ('invest' if [tp for tp in list_tp_invest if tp==x["dept_custo"]] else
                                        ('imprevisto' if [tp for tp in list_tp_imprevisto if tp==x["dept_custo"]] else 
                                        ('lazer' if [tp for tp in list_tp_lazer if tp==x["dept_custo"]] else 
                                            ('comercio' if [tp for tp in list_tp_comercio if tp==x["dept_custo"]] else
                                            ('carro' if [tp for tp in list_tp_carro if tp==x["dept_custo"]]  else 
                                                ('igreja' if [tp for tp in list_tp_igreja if tp==x["dept_custo"]]  else 
                                                ('poupanca' if x["dept_custo"]=='poupanca' else 'baixo'
                                    ))))))))), axis=1)

    return df

def func_generate_fix_type_cust(df):
    
    # print("DEFINIÇÃO DE TIPO DE CUSTO NULO","\n")

    #grupos 
    df["tipo_custo"] = df.apply(lambda x: x["tipo_custo"] if x["tipo_custo"]!=None and x["tipo_custo"]!="compras" else 
                                ( "agua" if x["descricao"].find('SABESP')>=0 else
                                ( "claro" if x["descricao"].find('DA  CLARO MOVEL')>=0 or x["descricao"].find('DA  CLARO CELULAR')>=0  else
                                ( "uber" if  x["descricao"].find('PIX QRS UBER')>=0 else
                                ( "escola_futebol" if x["descricao"].find('PIX TRANSF  REAL')>=0 or x["descricao"].find('PIX TRANSF  Real')>=0 else
                                ( "net" if x["descricao"].find('DA  NET SERVICOS')>=0 or x["descricao"].find('DA  CLARO RESIDENCIAL')>=0 or x["descricao"].find('DA  CLARO RESID')>=0 else
                                ( "gas" if x["descricao"].find('DA  COMGAS')>=0 else
                                ( "luz" if x["descricao"].find('DA  ELETROPAULO')>=0 else
                                ( "boleto" if x["descricao"].find('PAG BOLETO  PAG TIT BANC')>=0 or x["descricao"].find('PAG BOLETO  CONDOMINIO')>=0 else
                                ( "pessoal" if x["descricao"].find('PIX TRANSF  ANDRE')>=0 or x["descricao"].find('PIX TRANSF  Andre N')>=0 or x["descricao"].find('PIX QRS ANDRE NASCI')>=0 else
                                ( "shopee-app" if x["descricao"].find('Pix Sbf Comercio')>=0 else
                                ( "michelle-estetica" if x["descricao"].find('48.440.591 S-ct')>=0 else
                                ("boticario" if x["descricao"].find('Hna')>=0 else
                                ("Openai" if x["descricao"].find('Openai')>=0 else
                                ("certificado" if x["descricao"].find('Certificador')>=0 else
                                ("roupa" if x["descricao"].find('LOJAO DO BRASSAO PAULOBR')>=0 else
                                ("roupa" if x["descricao"].find('PURA ONDA ARTIGOS ESPOSAO PAULOBR')>=0 else
                                ( "emprestimo" if x["descricao"].find('PIX TRANSF  ELIZAB')>=0 else
                                ( "dora" if x["descricao"].find('PIX TRANSF  DORALIC')>=0 else
                                ( "igreja" if x["descricao"].find('PIX TRANSF  AGEMIW')>=0 else
                                ( "pastor" if x["descricao"].find('PIX TRANSF  ELIAS R')>=0 else
                                ( "notebook" if x["descricao"].find('PIX TRANSF  SAFIRA')>=0 else
                                ( "futebol_amigos" if x["descricao"].find('PIX TRANSF  LUCIANO')>=0 else
                                ( "primo_leonard" if x["descricao"].find('PIX TRANSF  LEONARD')>=0 or x["descricao"].find('PIX TRANSF  Leonard')>=0 else
                                ( "transporte_pub" if x["descricao"].find('PIX TRANSF  EMBRYO')>=0 else
                                ( "pessoa" if x["descricao"].find('Bruno Podero')>=0 or x["descricao"].find('Roberto')>=0 else
                                ( "alimentacao" if x["descricao"].find('RESTAURANTE')>=0  or x["descricao"].find('Paleteria')>=0 or x["descricao"].find('Matheus')>=0 else
                                ( "banco" if x["descricao"].find('TAR PACOTE ITAU')>=0  or x["descricao"].find('IOF')>=0 or x["descricao"].find('iof')>=0 or x["descricao"].find('Iof')>=0 or x["descricao"].find('Iof Parcelamento')>=0 or  x["descricao"].find('Juros De Mora')>=0 or x["descricao"].find('Juros De Mora')>=0 else 
                                ( "americanas" if x["descricao"].find('Ame*americanas')>=0 or  x["descricao"].find('Lojas Americ')>=0  else
                                ( "gasolina" if x["descricao"].find('Auto Posto')>=0 or x["descricao"].find('Posto Estacao')>=0 or x["descricao"].find('Posto Pe De Boi')>=0  else
                                ( "farmacia" if x["descricao"].find('Drogaria')>=0 or x["descricao"].find('Extrafarma')>=0 or x["descricao"].find('Raia')>=0 else 
                                ( "roupa" if x["descricao"].find('Centauro')>=0 or x["descricao"].find('Outlet')>=0  or x["descricao"].find('Pernambucanas')>=0  or x["descricao"].find('Cea')>=0 or x["descricao"].find('Decathlon')>=0 or x["descricao"].find('Hurley')>=0 or x["descricao"].find('Khelf')>=0 else
                                ( "comercio" if x["descricao"].find('Centro')>=0 or x["descricao"].find('Fernandinhos')>=0 or x["descricao"].find('Brands')>=0 or  x["descricao"].find('Y Model')>=0 or x["descricao"].find('ORION')>=0 else
                                ( "sorvete" if x["descricao"].find('Chiquinho')>=0 or x["descricao"].find('Pag*oggisorvetes')>=0 or x["descricao"].find('Oggi')>=0  else                                
                                ( "cabelereiro" if x["descricao"].find('erlonds')>=0 or x["descricao"].find('PIX TRANSF  ERLONDS')>=0 else 
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
                                ( "app-google" if x["descricao"].find('Dl*google Google')>=0 or x["descricao"].find('Dl     *google Youtube')>=0 else
                                ( "plataforma-gcp" if x["descricao"].find('Dl*google Cloud')>=0 or x["descricao"].find('Google Cloud')>=0 or x["descricao"].find('Dl *google Cloud 9g8vs')>=0 else
                                ( "app-spotify" if x["descricao"].find('Dm*spotify')>=0 or x["descricao"].find('Ebn         *spotify')>=0 or x["descricao"].find('Ebanx*spotify')>=0 else
                                ( "lojas-iplace" if x["descricao"].find('Lojas Iplace')>=0 else
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
                                None))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
                                ,axis=1)
                                # ( "escola_futebol" if  x["descricao"].find('Chute Inicial')>=0 or x["descricao"].find('R9 Parque')>=0 or x["descricao"].find('Real Brasil')>=0 or  x["descricao"].find('REAL BRASIL')>=0 or x["descricao"].find('PIX TRANSF  REAL')>=0 or x["descricao"].find('NIRLEY')>=0 else
                                # ( "futebol" if x["descricao"].find('Joadson')>=0 else
 
    return df
