variable "project" {
  description = "Google Cloud project ID"
  default     = "books-pipeline-490008"
}

variable "region" {
  description = "Compute region for resources"
  default     = "europe-west10"
}

variable "location" {
  description = "Location for data resources (GCS, BigQuery)"
  default     = "europe-west10"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset for Books, Users, Ratings"
  default     = "books_dataset_project"
}

variable "gcs_bucket_name" {
  description = "Storage Bucket for Books Data"
  default     = "kestra-books-bucket-latypov"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "credentials" {
  description = "My Google Cloud Account credentials"
  default     = "../service-account.json"
}


variable "kaggle_username" {
  description = "Kaggle API Username"
  sensitive   = true
}

variable "kaggle_key" {
  description = "Kaggle API Key"
  sensitive   = true
}