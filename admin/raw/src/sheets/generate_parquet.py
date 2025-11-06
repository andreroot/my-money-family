import pandas as pd

def generate_parquet_credito(df, ano):

    # df = pd.read_csv(f"./output/credito_{ano}.csv",
    #     sep=';',
    #     quotechar='"',
    #     encoding='utf-8',
    #     parse_dates=['data_base'],
    #     dayfirst=True,
    #     infer_datetime_format=True,
    #     keep_default_na=False
    #     )

    # print(df.columns)

    # opcional: adiciona year/month para particionamento
    df['year'] = df['data_base'].dt.year
    df['month'] = df['data_base'].dt.month

    # escreve particionado no S3
    df.to_parquet(
        f"s3://medalion-cust/processed/credito_{ano}.parquet",
        engine="pyarrow",
        compression="snappy",
        index=False,
        # partition_cols=['year', 'month'],
        storage_options={}  # deixe vazio se AWS credentials estiverem no ambiente
    )


def generate_parquet_debito(df, ano):

    # df = pd.read_csv(f"./output/extrato_{ano}.csv",
    #     sep=',',
    #     encoding='utf-8',
    #     parse_dates=['data_base'],
    #     dayfirst=True,
    #     infer_datetime_format=True,
    #     keep_default_na=False
    #     )

    # print(df.columns)

    # opcional: adiciona year/month para particionamento
    df['year'] = df['data_base'].dt.year
    df['month'] = df['data_base'].dt.month

    # escreve particionado no S3
    df.to_parquet(
        f"s3://medalion-cust/processed/extrato_{ano}.parquet",
        engine="pyarrow",
        compression="snappy",
        index=False,
        # partition_cols=['year', 'month'],
        storage_options={}  # deixe vazio se AWS credentials estiverem no ambiente
    )


def generate_parquet_analytics(df, file):

    # escreve particionado no S3
    df.to_parquet(
        f"s3://medalion-cust/processed/{file}.parquet",
        engine="pyarrow",
        compression="snappy",
        index=False,
        # partition_cols=['year', 'month'],
        storage_options={}  # deixe vazio se AWS credentials estiverem no ambiente
    )


# if __name__=='__main__':

#     generate_credito('2025')
#     generate_credito('2024')
#     generate_debito('2025')