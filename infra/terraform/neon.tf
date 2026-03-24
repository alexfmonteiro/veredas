# -----------------------------------------------------------------------------
# Neon — Serverless Postgres for metadata, insights, and query logs
# -----------------------------------------------------------------------------

resource "neon_project" "main" {
  name      = var.neon_project_name
  region_id = var.neon_region

  default_endpoint_settings {
    autoscaling_limit_min_cu = 0.25
    autoscaling_limit_max_cu = 0.25 # Free tier limit
    suspend_timeout_seconds  = 300  # Suspend after 5 min idle
  }
}

resource "neon_database" "app" {
  project_id = neon_project.main.id
  branch_id  = neon_project.main.default_branch_id
  name       = var.neon_database_name
  owner_name = neon_project.main.database_user
}
