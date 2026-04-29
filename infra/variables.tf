# infra/variables.tf

# ── Neon ──────────────────────────────────────────────────────
variable "neon_api_key" {
  description = "Neon API key (https://console.neon.tech/app/settings/api-keys)"
  type        = string
  sensitive   = true
}

variable "neon_project_name" {
  description = "Display name for the Neon project"
  type        = string
  default     = "market-pulse"
}

variable "neon_region_id" {
  description = "Neon region ID (e.g. aws-us-east-1, aws-us-east-2, aws-eu-central-1)"
  type        = string
  default     = "aws-us-east-1"
}

variable "neon_role_name" {
  description = "Postgres role used by the application"
  type        = string
  default     = "market_pulse_app"
}

variable "neon_database_name" {
  description = "Postgres database name"
  type        = string
  default     = "market_pulse_dw"
}

# ── Cloudflare R2 ─────────────────────────────────────────────
variable "cloudflare_api_token" {
  description = "Cloudflare API token with R2 edit permission"
  type        = string
  sensitive   = true
}

variable "cloudflare_account_id" {
  description = "Cloudflare account ID (dashboard → right sidebar)"
  type        = string
}

variable "r2_bucket_name" {
  description = "R2 bucket for raw Parquet landing"
  type        = string
  default     = "market-pulse-raw"
}

variable "r2_location" {
  description = "R2 location hint (ENAM, WNAM, EEUR, WEUR, APAC, OC)"
  type        = string
  default     = "ENAM"
}
