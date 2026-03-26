
import pandas as pd
import os
import datetime as dt    

def generate_type_cust_none(df):

    print(f"-->SAIDA NULO / CUSTO NULO / PULLING", "\n")
    # definição do contexto nos dados identifcar como custo
    df = df.loc[df['valor_custo'] < 0].copy()
    df = df.loc[df['descricao'].str.contains(r'SALDO[^\\b]+\w|APL[^\\b]APLIC[^\\b]AUT[^\\b]MAIS|RES[^\\b]APLIC[^\\b]AUT[^\\b]MAIS|REND[^\\b]PAGO[^\\b]APLIC[^\\b]AUT[^\\b]MAIS')==False].copy()
    
    # transform dados + colunas
    df = func_generate_sheets_type_cust(df)
    df = func_generate_fix_type_cust(df)

    if len(df[(df["tipo_custo"].isnull())])==0:
        print(f"-->SAIDA NULO / CUSTO NULO / PULLING", "\n")
        print("NENHUM TIPO DE CUSTO NULO PARA GERAR!!!")
        return df
    else:
        print(f'Total tipos_encontrados:{len(df[(df["tipo_custo"].notnull())])} / tipos_nao_encontrados:{len(df[(df["tipo_custo"].isnull())])}\n')



