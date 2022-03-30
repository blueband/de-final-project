
# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name     = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location = var.region

  # Optional, but recommended settings:
  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // days
    }
  }

  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}


# Analytic Machine for Spark/Pyspark 
module "dataproc" {
  source            = "./modules/dataproc"
  final_project     = var.final_project
  boot_disk_size_gb = var.boot_disk_size_gb
  image_version     = var.image_version
  dataproc_name     = var.dataproc_name
  account_key       = var.account_key
  zone              = var.zone
  # staging_bucket    = var.staging_bucket
  machine_type      = var.machine_type
  label             = var.label


}