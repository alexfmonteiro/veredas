# -----------------------------------------------------------------------------
# Upstash — Serverless Redis for rate limiting and caching
# -----------------------------------------------------------------------------

resource "upstash_redis_database" "cache" {
  database_name = "br-economic-pulse-${var.environment}"
  region        = var.upstash_region
  tls           = true
  eviction      = true # Evict keys when memory is full (LRU)
}
