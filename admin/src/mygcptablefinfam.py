from google.cloud import bigquery


#
# conceito de push: empurrar algo, são os gastos empurrados para fora do caixa financeiro para algum destino, são o destino final do fluxo
#

def insert_df_pushout(df, schema_path, table_id):
    from mygcpcredencial import my_credencial
    from google.cloud import bigquery
    
    credentials = my_credencial()
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project='devsamelo2')
    # TODO(developer): Set table_id to the ID of the table to create.
    
    # To load a schema file use the schema_from_json method.
    schema = client.schema_from_json(schema_path)

    try:
        table = client.get_table(table_id)  # API Request
        print("Table {} already exists.".format(table_id))
    except:

        print("Table {} is not found.".format(table_id))
        # [END bigquery_table_exists]
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema = [
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
        bigquery.SchemaField("tipo_custo", bigquery.enums.SqlTypeNames.STRING),#{'name': 'code', 'type': 'STRING', 'mode': 'nullable'},
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("custo", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("valor_custo", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("dt_mes_base", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("dt_custo", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("process_time", bigquery.enums.SqlTypeNames.TIMESTAMP)
    ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        #write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.

    if table.num_rows>0:
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    elif table.num_rows==0:
        print("tabela não gerada, VERIFICAR!!!")


#
# conceito de pull: puxar algo, são os recebidos puxados para o caixa financeiro, são a origem do fluxo
#

def insert_df_pulling(df, schema_path, table_id):
    # Construct a BigQuery client object.
    from mygcpcredencial import my_credencial
    from google.cloud import bigquery
    
    credentials = my_credencial()
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project='devsamelo2')

    schema = client.schema_from_json(schema_path)
    try:
        table = client.get_table(table_id)  # API Request
        print("Table {} already exists.".format(table_id))
    except:
        print("Table {} is not found.".format(table_id))
        # [END bigquery_table_exists]
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema = [
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.

        bigquery.SchemaField("descricao", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("valor_recebido", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("dt_mes_base", bigquery.enums.SqlTypeNames.DATE),
        #{'name': 'code', 'type': 'STRING', 'mode': 'nullable'},
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("dt_recebido", bigquery.enums.SqlTypeNames.DATE),      
        bigquery.SchemaField("process_time", bigquery.enums.SqlTypeNames.TIMESTAMP) 

    ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        #write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.

    if table.num_rows>0:
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    elif table.num_rows==0:
        print("tabela não gerada, VERIFICAR!!!")
    

#
# conceito de stop: parado em algum lugar, saldo do caixa financeiro para inicio de um novo ciclo do fluxo
#

def insert_df_stop(df, schema_path, table_id):
    # Construct a BigQuery client object.
    from mygcpcredencial import my_credencial
    from google.cloud import bigquery
    
    credentials = my_credencial()
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project='devsamelo2')

    schema = client.schema_from_json(schema_path)


    try:
        table = client.get_table(table_id)  # API Request
        print("Table {} already exists.".format(table_id))
    except:
        print("Table {} is not found.".format(table_id))
        # [END bigquery_table_exists]
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema = [
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.

        bigquery.SchemaField("descricao", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("saldo", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("dt_mes_base", bigquery.enums.SqlTypeNames.DATE),
        #{'name': 'code', 'type': 'STRING', 'mode': 'nullable'},
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("dt_recebido", bigquery.enums.SqlTypeNames.DATE),           
        bigquery.SchemaField("process_time", bigquery.enums.SqlTypeNames.TIMESTAMP) 
    ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        #write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.

    if table.num_rows>0:
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    elif table.num_rows==0:
        print("tabela não gerada, VERIFICAR!!!")
    

def insert_df_credits(df, schema_path, table_id):
    # Construct a BigQuery client object.
    from mygcpcredencial import my_credencial
    from google.cloud import bigquery
    
    credentials = my_credencial()
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project='devsamelo2')

    schema = client.schema_from_json(schema_path)

    try:
        table = client.get_table(table_id)  # API Request
        print("Table {} already exists.".format(table_id))
    except:
        print("Table {} is not found.".format(table_id))
        # [END bigquery_table_exists]
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema = [
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
        bigquery.SchemaField("tipo_custo_credito", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("custo_credito", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("valor_credito", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("valor_credito_parc", bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField("dt_mes_base", bigquery.enums.SqlTypeNames.DATE),
        #{'name': 'code', 'type': 'STRING', 'mode': 'nullable'},
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("dt_credito", bigquery.enums.SqlTypeNames.DATE),      
        bigquery.SchemaField("process_time", bigquery.enums.SqlTypeNames.TIMESTAMP) 

    ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        #write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.

    if table.num_rows>0:
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    elif table.num_rows==0:
        print("tabela não gerada, VERIFICAR!!!")

def insert_bucket_tabela():

    from mygcpcredencial import my_credencial
    from google.cloud import bigquery
    
    credentials = my_credencial()
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project='devsamelo2')

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = 'devsamelo2'

    # Set the encryption key to use for the destination.
    # TODO: Replace this key with a key you have created in KMS.
    # kms_key_name = "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}".format(
    #     "cloud-samples-tests", "us", "test", "test"
    # )
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        field_delimiter=";",
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    #uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
    uri = "gs://"
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))
