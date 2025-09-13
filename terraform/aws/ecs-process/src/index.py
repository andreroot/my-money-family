import json
import boto3
import pandas as pd
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        bucket = body.get("bucket", os.environ["BUCKET_NAME"])
        key = body.get("file", os.environ["FILE_KEY"])
        sheet_id = os.environ["SHEET_ID"]

        s3 = boto3.client("s3")
        tmp_file = f"/tmp/{key.split('/')[-1]}"
        s3.download_file(bucket, key, tmp_file)

        df = pd.read_csv(tmp_file)

        # Exemplo de processamento
        result = df.groupby("categoria")["valor"].sum().reset_index()

        creds_dict = json.loads(os.environ["GOOGLE_CREDS_JSON"])
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )

        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        values = [result.columns.tolist()] + result.values.tolist()
        body = {"values": values}

        sheet.values().update(
            spreadsheetId=sheet_id,
            range="Página1!A1",
            valueInputOption="RAW",
            body=body
        ).execute()

        return {"statusCode": 200, "body": json.dumps({"message": "Planilha atualizada com sucesso!"})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
