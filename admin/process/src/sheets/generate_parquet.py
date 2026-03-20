
import pandas as pd
import os
import datetime as dt    

def generate_parquet_analytics(df, tipo_docu, file):

    # escreve particionado no S3
    df.to_parquet(
        f"s3://medalion-cust/gold/analytics/{tipo_docu}/{file}.parquet",
        engine="pyarrow",
        compression="snappy",
        index=False,
        # partition_cols=['year', 'month'],
        storage_options={}  # deixe vazio se AWS credentials estiverem no ambiente
    )
