
import google.auth
import os

def my_credencial():


    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/andre/.ssh/my-chave-gcp-devsamelo2.json'

    credentials, project = google.auth.default(
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/bigquery"]
    )

    return credentials


def cred():
    
    chave = r'C:\Users\andre\.ssh\my-chave-gcp-devsamelo2.json'

    user = os.getlogin()
    #linux
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/andre/.ssh/my-chave-gcp-devsamelo2.json'
    #windows
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = chave
    #[Environment]::SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS","C:\Users\andre\.ssh\my-chave-gcp-devsamelo2.json","User")

    #credencial = service_account.Credentials.from_service_account_file(key)
    credencial, pid = google.auth.default(     
        scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",  
        "https://spreadsheets.google.com/feeds"  
    ])

    return credencial, pid

