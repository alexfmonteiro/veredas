terraform {
  required_version = ">= 1.5"

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.0"
    }

    neon = {
      source  = "kislerdm/neon"
      version = "~> 0.6"
    }

    upstash = {
      source  = "upstash/upstash"
      version = "~> 1.0"
    }

    sentry = {
      source  = "jianyuan/sentry"
      version = "~> 0.14"
    }
  }
}
