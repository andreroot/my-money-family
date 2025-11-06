from pydantic import BaseModel, Field, validator
import pandas as pd
import re
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


    @validator('valor_custo', 'valor_receb', 'valor_saldo', pre=True)
    def parse_float(cls, v):
        """
        Aceita números, strings vazias, formatos BR (1.234,56), 'R$ 1.234,56' e parênteses para negativos.
        Strings vazias -> 0.0 (ajuste se preferir None).
        """
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if s == '':
            return 0.0
        # trata parênteses como negativo: (1.234,56)
        neg = False
        if s.startswith('(') and s.endswith(')'):
            neg = True
            s = s[1:-1].strip()
        # remove símbolos de moeda e espaços
        s = re.sub(r'[^\d,.\-]', '', s)
        # se houver ambos . e , assume formato brasileiro (milhar '.' e decimal ',')
        if '.' in s and ',' in s:
            s = s.replace('.', '').replace(',', '.')
        else:
            # trocar vírgula decimal por ponto
            s = s.replace(',', '.')
        try:
            val = float(s)
        except Exception:
            raise ValueError(f"Não foi possível converter '{v}' para float")
        return -val if neg else val

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

    print(df.columns)
    print(df.dtypes)
    



    try:
        # Valide e converta cada linha para o modelo Pydantic
        registros = [CustoSchema(**row) for row in df.to_dict(orient="records")]
        print(registros[0])
        print(type(registros))

        # Serialização para JSON (opcional)
        # registros_serializados = [r.model_dump_json() for r in registros[0]]
        # print(registros_serializados)

        # Desserialização de JSON (opcional)
        # registros_desserializados = [CustoSchema.model_validate_json(rj) for rj in registros_serializados]
        # print(registros_desserializados)

        # Agora você pode usar os objetos validados ou exportar novamente para CSV
        df_validado = pd.DataFrame([r.dict() for r in registros])

        df_validado.to_csv("./output/model.csv", index=False)

        return df_validado

    except ValueError as e:
        print(f"Erro de validação: {e}")
        return None
