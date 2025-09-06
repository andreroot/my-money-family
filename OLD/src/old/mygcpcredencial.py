def my_credencial():

    import google.auth
    import os

    #linux
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/andre/.ssh/my-chave-gcp-devsamelo2.json'
    #windows
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'C:\Users\andre\.ssh\my-chave-gcp-devsamelo2.json'

    # credentials, project = google.auth.default(
    #     scopes = ["https://www.googleapis.com/auth/spreadsheets",
    #                 "https://www.googleapis.com/auth/drive",
    #                 "https://spreadsheets.google.com/feeds",
    #                 "https://www.googleapis.com/auth/cloud-platform",
    #                 "https://www.googleapis.com/auth/bigquery"]
    # )

    credencial, pid = google.auth.default(     
        scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/bigquery"
    ])

    return credencial
