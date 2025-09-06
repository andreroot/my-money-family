resource "google_storage_bucket" "bq_raw" {
  name     = "pipeline-data-raw-andre-20250823"
  location = "US"
  force_destroy = true
}


# resource "google_storage_bucket_object" "custo_folder" {
#   name   = "custo/" # Cria um "folder" chamado custo dentro do bucket raw
#   bucket = google_storage_bucket.bq_raw.name
#   content = ""
# }

variable "arquivos" {
  type    = list(string)
  default = [
    "custo_2025_01.xls",
    "custo_2025_02.xls",
    "custo_2025_03.xls",
    "custo_2025_04.xls",
    "custo_2025_05.xls",
    "custo_2025_06.xls",
    "custo_2025_07.xls",
    "custo_2025_08.xls",
    "custo_2025_09.xls",
    "custo_2025_10.xls",
    "custo_2025_11.xls",
    "custo_2025_12.xls"            
  ]
}

resource "google_storage_bucket_object" "custo_files" {
  for_each = toset(var.arquivos)
  name     = "custo/${each.value}"
  bucket   = google_storage_bucket.bq_raw.name
  source   = "${path.module}/arquivos/raw/${each.value}"
}

resource "google_storage_bucket" "bq_process" {
  name     = "pipeline-data-process-andre-20250823"
  location = "US"
  force_destroy = true
}


variable "arquivos_p" {
  type    = list(string)
  default = [
    "extrato_2025.csv"
  ]
}

resource "google_storage_bucket_object" "custo_p_files" {
  for_each = toset(var.arquivos_p)
  name     = "custo/${each.value}"
  bucket   = google_storage_bucket.bq_process.name
  source   = "${path.module}/arquivos/process/${each.value}"
}
