# ETL dos arquivos baixados do itau

## RAW
Execução do docker na AWS via Tasks ECS


**1. Processo de prepararação de docker image**

```bash

executar deploy / gerar build da image
raw/deploy_docker_ecs.sh

```

- arquivo baixados estao localmente são trasnferidos para S3
- login no ecs e atualizar image na aws ecs
- build image
- cp  ~/.ssh/my-chave-gcp-devsamelo2.json .
- criar tag latest
- copia image para ecs
- executa docker via task ecs
    curl -X POST "https://0cgzijkxda.execute-api.us-east-1.amazonaws.com/run"

**2. Execução do docker**

```bash

Inicio
./execute.sh 

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
    curl -X POST "https://blgfx6i8j3.execute-api.us-east-1.amazonaws.com/run"

**2. Execução da Transformação**

```bash

executar deploy / gerar build da image
python ./admin/process/src/main.py 

```

Gerando informações no sheets

