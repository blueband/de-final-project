resource "google_service_account" "default" {
  account_id   = var.account_key
  display_name = "de-zoom-final-project-service-account"
}

resource "google_dataproc_cluster" "de-cluster" {
  name                          = var.dataproc_name
  region                        = var.zone
  graceful_decommission_timeout = "120s"
  labels = {
    project = var.label
  }

  cluster_config {
    # staging_bucket = var.staging_bucket

    master_config {
      num_instances = 1
      machine_type  = var.machine_type
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = var.boot_disk_size_gb
      }
    }

    # worker_config {
    #   num_instances    = 2
    #   machine_type     = "e2-medium"
    #   min_cpu_platform = "Intel Skylake"
    #   disk_config {
    #     boot_disk_size_gb = 30
    #     num_local_ssds    = 1
    #   }
    # }

    preemptible_worker_config {
      num_instances = 0
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
      service_account = google_service_account.default.email
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