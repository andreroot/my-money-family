import pandas as pd
import os
import datetime as dt  
import re  

# sheets
import gspread
from gspread_dataframe import get_as_dataframe



def connect():
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './.ssh/my-chave-gcp-devsamelo2.json'
    gc = gspread.service_account(filename=os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    return gc

def get_depara_tipo_custo():

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

    dftype[['de_para','valor']].to_json("./output/depara_csv.json", orient="records", force_ascii=False, indent=2)

    return list_tp