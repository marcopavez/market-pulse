# infra/main.tf
#
# Provisions the free-tier stack for Market Pulse:
#   - Neon: Postgres project + database + role
#   - Cloudflare R2: object storage bucket (S3-compatible)
#
# State is stored locally — no remote backend (free-tier friendly).
# Re-running `terraform apply` is idempotent.

terraform {
  required_version = ">= 1.5"

  required_providers {
    neon = {
      source  = "kislerdm/neon"
      version = "~> 0.6"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.40"
    }
  }
}

# ── Neon (Postgres) ───────────────────────────────────────────
provider "neon" {
  api_key = var.neon_api_key
}

resource "neon_project" "main" {
  name      = var.neon_project_name
  region_id = var.neon_region_id
  pg_version = 16
}

resource "neon_role" "app" {
  project_id = neon_project.main.id
  branch_id  = neon_project.main.default_branch_id
  name       = var.neon_role_name
}

resource "neon_database" "warehouse" {
  project_id = neon_project.main.id
  branch_id  = neon_project.main.default_branch_id
  name       = var.neon_database_name
  owner_name = neon_role.app.name
}

# ── Cloudflare R2 (object storage) ────────────────────────────
provider "cloudflare" {
  api_token = var.cloudflare_api_token
}

resource "cloudflare_r2_bucket" "raw" {
  account_id = var.cloudflare_account_id
  name       = var.r2_bucket_name
  location   = var.r2_location
}
