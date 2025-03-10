terraform {
 backend "gcs" {
   bucket  = google_storage_bucket.name
   prefix  = "terraform/state/staging"
 }
}