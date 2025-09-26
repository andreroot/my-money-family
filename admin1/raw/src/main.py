import pandas as pd
import os
import datetime as dt    

# model
from model.model import model_data
from model.modelcred import model_data as model_data_cred

# generate 
from sheets.generate_data import generate, generate_cred, generate_type_cust_none, pushout, pulling, stop, pushout_cred

def main(): 

    # generate
    df = pd.DataFrame()

    df = generate()

    df = model_data(df)

    df["valor_custo"] = df["valor_custo"].fillna(0)
    df["valor_saldo"] = df["valor_saldo"].fillna(0)

    now = dt.datetime.now()
    dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

    df["process_time"] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")

    return df

if __name__=='__main__':

    #
    df = pd.DataFrame()
    df = main()

    # custo
    pushout(df)

    # receber
    pulling(df)

    # saldo
    stop(df)

    # credito
    df = pd.DataFrame()

    df = generate_cred()
    df = model_data_cred(df)

    now = dt.datetime.now()
    dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

    df["process_time"] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")

    pushout_cred(df)
