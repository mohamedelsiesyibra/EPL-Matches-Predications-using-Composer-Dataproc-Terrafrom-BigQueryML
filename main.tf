terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.85.0"
    }
  }
}

# Load and decode the service account key file
locals {
  credentials = jsondecode(file("keys.json"))
}

provider "google" {
  credentials = file("keys.json")
  project     = local.credentials.project_id
  region      = "us-east1"
}

# Enable cloud composer API
resource "google_project_service" "composer_api" {
  service = "composer.googleapis.com"

  disable_on_destroy = false
}

# Enable cloud dataproc API
resource "google_project_service" "dataproc_api" {
  service = "dataproc.googleapis.com"

  disable_on_destroy = false
}

# Create a bucket for dataproc
resource "google_storage_bucket" "dataproc_bucket" {
  name     = "wr-dataproc"
  location = "us"
  force_destroy = true
}

# Create the project main bucket
resource "google_storage_bucket" "project_bucket" {
  name     = "wr-epl-predictions-project"
  location = "us"
  force_destroy = true
}

# Upload processing.py
resource "google_storage_bucket_object" "processing_file" {
  name   = "dataproc-scripts/processing.py"
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "dataproc/process-staging-json-files.py"
}

# Upload the sql queries for BigQuery
resource "google_storage_bucket_object" "bq_bqml_implementation" {
  name   = "wr-bq-temporary-bucket/bqml-implementation.sql"
  bucket = google_storage_bucket.project_bucket.name
  source = "bigquery/bqml-implementation.sql"
}

resource "google_storage_bucket_object" "bq_dateset_transformation" {
  name   = "wr-bq-temporary-bucket/dateset-transformation.sql"
  bucket = google_storage_bucket.project_bucket.name
  source = "bigquery/dateset-transformation.sql"
}

# Create a dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = "ML_epl"
  friendly_name = "ML_epl"
  description = "This dataset will be used for the BQML created model"
  location    = "US"
  delete_contents_on_destroy  = true

  labels = {
    env = "default"
  }
}

# Create the Cloud Composer environment
resource "google_composer_environment" "composer_env" {
  name   = "composer-environment"
  region = "us-west4"

  config {
    node_config {
      zone = "us-west4-a"
      machine_type = "n1-standard-1"
    }

    software_config {
      image_version = "composer-1.20.12-airflow-2.4.3"
      python_version = "3"

      airflow_config_overrides = {
        core-load_example = "True"
      }
    }
  }
}

# Add the airflow.py from local to the DAGs bucket
resource "google_storage_bucket_object" "airflow_dag" {
  # Extract bucket name only, without 'gs://' prefix and '/dags' postfix.
  bucket = replace(split("/", replace(google_composer_environment.composer_env.config.0.dag_gcs_prefix, "gs://", ""))[0], "/", "")

  # Use name for the full path within the bucket.
  name   = "dags/pyspark.py"

  source = "cloud-composer/dags/data-processing.py"
  depends_on = [google_composer_environment.composer_env]
}

