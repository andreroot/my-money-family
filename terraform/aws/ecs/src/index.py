import os
import boto3

ecs = boto3.client("ecs")

def lambda_handler(event, context):
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
        }
    )
    return {
        "statusCode": 200,
        "body": str(response)
    }
