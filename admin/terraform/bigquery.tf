resource "google_bigquery_dataset" "dataset_fin" {
  dataset_id                  = "dev_my_finance"
  friendly_name               = "myfin"
  description                 = "This is a test description"
  location                    = "US"
  #default_table_expiration_ms = 3600000

  labels = {
    env = "default"
  }
}

resource "google_bigquery_table" "tab_custo_fin" {
  dataset_id = google_bigquery_dataset.dataset_fin.dataset_id
  table_id   = "custo"
  

  #time_partitioning {
  #  type = "DAY"
  #}

  labels = {
    env = "default"
  }

  schema = <<EOF
  [
  {"name": "tipo_custo", "type":"STRING", "mode":"nullable"},
  {"name": "custo", "type":"STRING", "mode":"nullable"},            
  {"name": "valor_custo", "type":"FLOAT", "mode":"nullable"},
  {"name": "dt_mes_base", "type":"DATE", "mode":"nullable"},
  {"name": "dt_custo", "type":"DATE", "mode":"nullable"},
  {"name": "process_time", "type":"TIMESTAMP", "mode":"nullable"} 
  ]
  EOF

deletion_protection  = false
}

#resource "google_bigquery_table" "sheet" {
#  dataset_id = google_bigquery_dataset.default.dataset_id
#  table_id   = "sheet"
#
#  external_data_configuration {
#    autodetect    = true
#    source_format = "GOOGLE_SHEETS"
#
#    google_sheets_options {
#      skip_leading_rows = 1
#    }
#
#    source_uris = [
#      "https://docs.google.com/spreadsheets/d/123456789012345",
#    ]
#  }
#    tags = {
#    Environment = var.environment
#  }
#}