terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
    kestra = {
      source  = "kestra-io/kestra"
    }
    http = {
      source  = "hashicorp/http"
    }
  }
}

provider "google" {
  credentials = file("${path.module}/${var.credentials}")
  project     = var.project
  region      = var.region
}

# --- GCP RESOURCES ---
# Removed the 'data' blocks and 'count' logic. 
# Terraform will now manage these resources directly.

resource "google_storage_bucket" "books-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition { age = 3 }
    action    { type = "Delete" }
  }

  lifecycle_rule {
    condition { age = 1 }
    action    { type = "AbortIncompleteMultipartUpload" }
  }

  storage_class = var.gcs_storage_class
}

resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location

  lifecycle {
    prevent_destroy = false 
  }
}

# --- KESTRA PROVIDER ---

provider "kestra" {
  url      = "http://kestra:8080"
  username = "admin@kestra.io"
  password = "Admin1234!"
}

# --- KESTRA KV STORE ---
# Added 'type = "STRING"' to prevent truncation of values with dashes

resource "kestra_kv" "gcp_project_id" {
  namespace = "books_pipeline"
  key       = "GCP_PROJECT_ID"
  value     = var.project
  type      = "STRING"
}

resource "kestra_kv" "gcp_location" {
  namespace = "books_pipeline"
  key       = "GCP_LOCATION"
  value     = var.location
  type      = "STRING"
}

resource "kestra_kv" "gcp_bucket_name" {
  namespace = "books_pipeline"
  key       = "GCP_BUCKET_NAME"
  value     = var.gcs_bucket_name
  type      = "STRING"
}

resource "kestra_kv" "gcp_dataset" {
  namespace = "books_pipeline"
  key       = "GCP_DATASET"
  value     = var.bq_dataset_name
  type      = "STRING"
}

resource "kestra_kv" "gcp_creds" {
  namespace = "books_pipeline"
  key       = "GCP_CREDS"
  value     = file("${path.module}/${var.credentials}")
  type      = "JSON"
}

resource "kestra_kv" "kaggle_username" {
  namespace = "books_pipeline"
  key       = "KAGGLE_USERNAME"
  value     = var.kaggle_username
  type      = "STRING"
}

resource "kestra_kv" "kaggle_key" {
  namespace = "books_pipeline"
  key       = "KAGGLE_KEY"
  value     = var.kaggle_key
  type      = "STRING"
}