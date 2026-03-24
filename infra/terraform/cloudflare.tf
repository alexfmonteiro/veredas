# -----------------------------------------------------------------------------
# Cloudflare R2 — Object storage for pipeline data (medallion architecture)
#
# Prefix-based organization:
#   bronze/  — raw immutable ingestion data
#   silver/  — cleaned and enriched data
#   gold/    — production-ready aggregated data
#   quality/ — QualityReport artifacts
# -----------------------------------------------------------------------------

resource "cloudflare_r2_bucket" "data" {
  account_id = var.cloudflare_account_id
  name       = var.r2_bucket_name
  location   = "ENAM" # Eastern North America — closest to Neon us-east-1
}
