from pydantic import BaseModel, Field, validator
import pandas as pd
from datetime import datetime, date

class CustoSchema(BaseModel):
    dt_credito: date = Field(alias="dt_credito")
    descricao: str = Field(alias="descricao")
    valor_credito: float = Field(alias="valor_credito")
    data_base: date

    @validator('dt_credito', pre=True)
    def parse_date(cls, v):
        if isinstance(v, date):
            return v
        return datetime.strptime(v, "%d/%m/%Y").date()
    
    @validator('data_base', pre=True)
    def parse_data_base(cls, v):
        if isinstance(v, date):
            return v
        print(v)
        return datetime.strptime(v, "%d/%m/%Y").date()

def model_data(df):
    # Carregue o CSV
    # df = pd.read_csv("output/extrato_2025.csv")
    # print(df.columns)
    # df=df[['data', 'lançamento', 'ag./origem', 'valor (R$)', 'saldos (R$)',
    #        'nome_arquivo', 'data_base']]
    # Renomeie as colunas conforme o modelo Pydantic
    # df['valor'] = df['valor (R$)']
    
    df = df[['data', 'lançamento', 'valor', 'data_base']].rename(columns={
        "data": "dt_credito",
        "lançamento": "descricao",
        "valor": "valor_credito",

    })
    
    print(df.columns)
    
    df = df.dropna(subset=['data_base'])

    try:
        # usuario = Usuario(nome=123, idade=25.5, email=None)

        # Valide e converta cada linha para o modelo Pydantic
        registros = [CustoSchema(**row) for row in df.to_dict(orient="records")]  
        print(registros[0])
        print(type(registros))
        
        # Agora você pode usar os objetos validados ou exportar novamente para CSV
        df_validado = pd.DataFrame([r.dict() for r in registros])

        df_validado.to_csv("./output/model_cred.csv", index=False)

        return df_validado

    except ValueError as e:
        print(f"Erro de validação: {e}")
        return None

