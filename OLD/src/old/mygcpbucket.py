
def get_script_sql(file):
    from google.cloud import storage
    from mygcpcredencial import my_credencial

    credentials = my_credencial()
    # Construct a BigQuery client object.
    storage_client = storage.Client(credentials=credentials, project='devsamelo2')
    
    # create storage client
    # get bucket with name
    bucket = storage_client.get_bucket('proj-domestico-file')
    # get bucket data as blob
    # proj = projeto
    source_file_name = f'sql/{file}'
    blob = bucket.get_blob(source_file_name)
    # convert to string
    bcontent = blob.download_as_string()
    return bcontent.decode("utf8")