from pydantic import BaseModel, Field, validator
import pandas as pd
from datetime import datetime, date

class CustoSchema(BaseModel):
    dt_custo: date = Field(alias="dt_custo")
    descricao: str = Field(alias="descricao")
    valor_custo: float = Field(alias="valor_custo")
    valor_receb: float = Field(alias="valor_receb")
    valor_saldo: float = Field(alias="valor_saldo")
    data_base: date

    @validator('dt_custo', pre=True)
    def parse_date(cls, v):
        if isinstance(v, date):
            return v
        return datetime.strptime(v, "%d/%m/%Y").date()
    
    @validator('data_base', pre=True)
    def parse_data_base(cls, v):
        if isinstance(v, date):
            return v
        return datetime.strptime(v, "%d/%m/%Y").date()

def model_data(df):
    # Carregue o CSV
    # df = pd.read_csv("output/extrato_2025.csv")
    # print(df.columns)
    # df=df[['data', 'lançamento', 'ag./origem', 'valor (R$)', 'saldos (R$)',
    #        'nome_arquivo', 'data_base']]
    # Renomeie as colunas conforme o modelo Pydantic
    df['valor (R$) receb'] = df['valor (R$)']
    
    df = df[['data', 'lançamento', 'valor (R$)', 'valor (R$) receb', 'saldos (R$)', 'data_base']].rename(columns={
        "data": "dt_custo",
        "lançamento": "descricao",
        "valor (R$)": "valor_custo",
        "valor (R$) receb": "valor_receb",
        "saldos (R$)": "valor_saldo"
    })

    # print(df.columns)
    
    # Valide e converta cada linha para o modelo Pydantic
    registros = [CustoSchema(**row) for row in df.to_dict(orient="records")]

    # Agora você pode usar os objetos validados ou exportar novamente para CSV
    df_validado = pd.DataFrame([r.dict() for r in registros])

    df_validado.to_csv("/app/output/model.csv", index=False)

    return df_validado