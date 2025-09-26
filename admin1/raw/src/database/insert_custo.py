# Construct a BigQuery client object.
from credencial.credencial_gcp import my_credencial
from google.cloud import bigquery
#
# conceito de push: empurrar algo, são os gastos empurrados para fora do caixa financeiro para algum destino, são o destino final do fluxo
#

def insert_df_pushout(df, schema_path, table_id):
    
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