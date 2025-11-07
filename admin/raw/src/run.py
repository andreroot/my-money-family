import pandas as pd
# generate 
from sheets.get_data import read_parquet_debito as generate_debito
# model
from model.model import model_data

import asyncio

async def main_debito(ano): 

    # generate
    df = pd.DataFrame()

    df = generate_debito(ano)

    df = model_data(df)

    df["valor_custo"] = df["valor_custo"].fillna(0)
    df["valor_saldo"] = df["valor_saldo"].fillna(0)

    now = dt.datetime.now()
    dt_process = dt.datetime.fromtimestamp(dt.datetime.timestamp(now))

    df["process_time"] = dt_process #.strftime("%Y-%m-%d %H:%M:%S.%f %z")

    return df

async def main(dfd, ano):
    tasks = [
        asyncio.create_task(pushout(dfd, ano)),
        asyncio.create_task(pulling(dfd, ano)),
        asyncio.create_task(stop(dfd, ano))
    ]
    results = await asyncio.gather(*tasks)
    for result in results:
        print(result)

if __name__ == '__main__':

    dfd = pd.DataFrame()

    dfd = main_debito(ano)
    
    asyncio.run(main(dfd, ano="2025"))

# if __name__=='__main__':

#     # generate
#     df = pd.DataFrame()

#     df = generate_debito_sheets(ano="2025")
    
#     print(df.columns)