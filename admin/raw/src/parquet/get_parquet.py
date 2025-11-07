


import pandas as pd
import os
import datetime as dt    

def get_parquet_process(parquet_file: str = None, tipo_docu: str = None, s3_options: dict = None) -> pd.DataFrame:
 
    """
    Se s3_path for fornecido, lê parquet diretamente do S3 e retorna o DataFrame.
    Caso contrário, mantém o comportamento existente (ler do Google Sheets).

    Exemplo de chamada para S3:
      generate_process(s3_path="s3://medalion-cust/processed/credito_2025.parquet")

    Requisitos:
      pip install pyarrow s3fs
      Credenciais AWS pelo ambiente (AWS_* vars) ou role da instância/task.
    """

    s3_path=f"s3://medalion-cust/processed/{tipo_docu}/{parquet_file}.parquet"

    if s3_path:
        # leitura direta do parquet em S3 (pyarrow/s3fs)
        # s3_options é um dict opcional para storage_options do pandas (ex: {"key": "...", "secret": "..."})
        df = pd.read_parquet(s3_path, storage_options=s3_options or {}, engine="pyarrow")

        print(f"TOTAL DE LINHAS (S3): {len(df)} / PATH: {s3_path}", "\n")
        

    return df