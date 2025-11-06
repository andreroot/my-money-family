import pandas as pd
# generate 
from sheets.generate_data import generate_debito
# model
from model.model import model_data

if __name__=='__main__':

    # generate
    df = pd.DataFrame()

    df = generate_debito('2025')
    
    df = model_data(df)

    print(df.columns)