# ETL dos arquivos baixados do itau

## RAW
Execução do docker na AWS via Tasks ECS

```bash

cd my-money-family
source ./.venv/bin/activate
cd ./admin/raw/
./etl_sh/execute.sh 2026 debito

```

**v2 - 18-02 - credito - incluir parametro tipo do cartão**
```bash
aws s3 cp . s3://medalion-cust/raw/original/ --recursive --exclude "*" --include "credito_uniclass_black_*.csv"
aws s3 cp . s3://medalion-cust/raw/original/ --recursive --exclude "*" --include "credito_uniclass_signature_*.csv"

# uniclass_black
./etl_sh/execute.sh 2026 credito uniclass_black
# uniclass_signature
./etl_sh/execute.sh 2026 credito uniclass_signature


```

**1. Processo de prepararação de docker image**

```bash

executar deploy / gerar build da image
raw/deploy_docker_ecs.sh

```

- arquivo baixados estao localmente são transferidos para S3
- login no ecs e atualizar image na aws ecs
- build image
- criar tag latest
- copia image para ecs
- executa docker via task ecs
    curl -X POST "https://0cgzijkxda.execute-api.us-east-1.amazonaws.com/run"

    curl -X POST "https://0cgzijkxda.execute-api.us-east-1.amazonaws.com/run?ano=2025" \
    -H "Content-Type: application/json" \
    -d '{"ano": "2022"}'

    curl -X POST "https://0cgzijkxda.execute-api.us-east-1.amazonaws.com/run?ano=2025&type_doc=credito" \
    -H "Content-Type: application/json" \
    -d '{"ano": "2025", "type_doc": "credito", "extension_file": "xls"}'

**2. Execução do docker**
- recebe parametros de fora para dentro do lambda que executa os processos de etl
```bash

Inicio
./execute.sh PARAM_ANO PARAM_TIPO_DOC

    - base extrato da conta corrente
    ./etl.sh 

    - base custo cartão creditos
    ./etlcred.sh 

    - dentro da maq virtualenv 
    python ./admin/raw/src/main.py 
```

Gerando informações no sheets

     
 

## PROCESS
Execução local

**1. Processo de prepararação de docker image**

```bash

executar deploy / gerar build da image
process/deploy_docker_ecs.sh

```

- arquivo baixados estao localmente são trasnferidos para S3
- login no ecs e atualizar image na aws ecs
- build image
- criar tag latest
- copia image para ecs
- executa docker via task ecs
    curl -X POST "https://blgfx6i8j3.execute-api.us-east-1.amazonaws.com/run?ano=2025" \
    -H "Content-Type: application/json" \
    -d '{"ano": "2022"}'


**2. Execução da Transformação**

```bash

executar deploy / gerar build da image
python ./admin/process/src/main.py 

```

Gerando informações no sheets

