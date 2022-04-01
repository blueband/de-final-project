locals {
  data_lake_bucket = var.data_lake_bucket
}

variable "project" {
  description = "Your GCP Project ID"
  type        = string
}

variable "data_lake_bucket" {

}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string

}

variable "credentials" {
  type = string
}

variable "account_key" {
  type = string
}

variable "zone" {
  type = string
}

variable "final_project" {
  type = string

}

# variable "staging_bucket" {
#   type = string
# }

variable "machine_type" {
  type = string

}

variable "boot_disk_size_gb" {
  type = number

}

variable "image_version" {
  type = string
}

variable "dataproc_name" {
  type = string

}
variable "label" {

}

variable "service_account_email" {

}