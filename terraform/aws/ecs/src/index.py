import os
import boto3
import urllib.parse
import json

ecs = boto3.client("ecs")

def lambda_handler(event, context):

    print("Payload recebido:", event)

    # EXTRAIR PARAMETROS: url -d '{"ano":"2025"}'
    body_str = event.get('body', '')
    body = json.loads(body_str)
    ano = body['ano']
    extension_file = body['extension_file']
    type_doc = body['type_doc']

    print(f"Ano recebido na body: {ano}")
    print(f"Extensão do arquivo recebido na body: {extension_file}")
    print(f"Tipo de extrato recebido na body: {type_doc}")

    # EXTRAIR PARAMETROS: rawQueryString - url?ano=2025
    raw_query_string = event.get('rawQueryString', '')
    parsed_query = urllib.parse.parse_qs(raw_query_string)
    ano_query = parsed_query.get('ano', [''])[0]

    print(f'Ano recebido na query: {ano_query}')

    response = ecs.run_task(
        cluster=os.environ["CLUSTER_ARN"],
        launchType="FARGATE",
        taskDefinition=os.environ["TASK_DEFINITION_ARN"],
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": os.environ["SUBNETS"].split(","),
                "securityGroups": os.environ["SECURITY_GROUPS"].split(","),
                "assignPublicIp": "ENABLED"
            }
        },
        overrides={
            'containerOverrides': [
                {
                    'name': os.environ["NOME_CONTAINER"],
                    'environment': [
                        {'name': 'ANO', 'value': ano},
                        {'name': 'TYPE_DOC', 'value': type_doc}
                    ],
                    # Se quiser passar argumentos para o CMD:
                    'command': ['bash', './etl_sh/execute.sh', ano, type_doc]
                }
            ]
        }
    )
    return {
        "statusCode": 200,
        "body": str(response)
    }
