
from sheets.generate_data import generate_parquet_process


if __name__=='__main__':

    df = generate_parquet_process(parquet_file="extrato_2025")
    print(df)