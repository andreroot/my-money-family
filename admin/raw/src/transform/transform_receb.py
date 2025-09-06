import pandas as pd
import os
import datetime as dt  
import re  

# sheets
import gspread
from gspread_dataframe import get_as_dataframe


def func_transform_receb(df):

    # print("DEFINIÇÃO DOS RECEBIMENTOS","\n")

    #grupos 
    df["tipo_receb"] = df.apply(lambda x: "michelle" if x["descricao"].find('MICHELL')>=0 else
                                ("dev_uber" if x["descricao"].find('DEV')>=0 or x["descricao"].find('Uber')>=0 else
                                ( "salario" if x["descricao"].find('PAGTO SALARIO')>=0 or x["descricao"].find('PAGTO ADIANT SALARIAL')>=0 or x["descricao"].find('PAGTO 13. SALARIO')>=0 or x["descricao"].find('PAGTO FERIAS')>=0 or x["descricao"].find('PAGTO BONUS')>=0 else
                                ( "rendimento" if x["descricao"].find('REND')>=0 else
                                ( "pix" if x["descricao"].find('PIX')>=0 else
                                ( "caixa" if x["descricao"].find('CAIXA')>=0 else
                                None))))),axis=1)
                                # ( "escola_futebol" if  x["descricao"].find('Chute Inicial')>=0 or x["descricao"].find('R9 Parque')>=0 or x["descricao"].find('Real Brasil')>=0 or  x["descricao"].find('REAL BRASIL')>=0 or x["descricao"].find('PIX TRANSF  REAL')>=0 or x["descricao"].find('NIRLEY')>=0 else
                                # ( "futebol" if x["descricao"].find('Joadson')>=0 else
 
    return df
