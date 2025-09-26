from pydantic import BaseModel, Field, validator
import pandas as pd
from datetime import datetime, date

class AnalyticsSchema(BaseModel):
    mes: str = Field(alias="mes")
    saldo: float = Field(alias="saldo")
    entrada: float = Field(alias="entrada")
    saida: float = Field(alias="saida")
    previsao_saldo_fim: float = Field(alias="previsao_saldo_fim")
    analytics: str = Field(alias="analytics")

    # @validator('dt_custo', pre=True)
    # def parse_date(cls, v):
    #     if isinstance(v, date):
    #         return v
    #     return datetime.strptime(v, "%d/%m/%Y").date()
    
    # @validator('data_base', pre=True)
    # def parse_data_base(cls, v):
    #     if isinstance(v, date):
    #         return v
    #     return datetime.strptime(v, "%d/%m/%Y").date()

def model_data(df):
   
    # df = df.rename(columns={

    # })

    # print(df.columns)
    
    # Valide e converta cada linha para o modelo Pydantic
    registros = [AnalyticsSchema(**row) for row in df.to_dict(orient="records")]

    # Agora você pode usar os objetos validados ou exportar novamente para CSV
    df_validado = pd.DataFrame([r.dict() for r in registros])

    df_validado.to_csv("unido_renomeado.csv", index=False)

    return df_validado