resource "google_dataproc_cluster" "de-cluster" {
  name                          = var.dataproc_name
  region                        = var.zone
  graceful_decommission_timeout = "120s"
  labels = {
    project = var.label
  }

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = var.machine_type
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = var.boot_disk_size_gb
      }
    }

    # Override or set some custom properties
    software_config {
      image_version = var.image_version
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      tags = ["de-zoom", "final-project"]
      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
      service_account = var.service_account_email
      service_account_scopes = [
        "cloud-platform"
      ]
    }

    # You can define multiple initialization_action blocks
    # initialization_action {
    #   script      = "gs://dataproc-initialization-actions/stackdriver/stackdriver.sh"
    #   timeout_sec = 500
    # }
  }
}