# infra/outputs.tf

output "neon_host" {
  description = "Neon Postgres host (use in POSTGRES_URL)"
  value       = neon_project.main.database_host
}

output "neon_database" {
  description = "Neon database name"
  value       = neon_database.warehouse.name
}

output "neon_role" {
  description = "Neon Postgres role"
  value       = neon_role.app.name
}

output "neon_password" {
  description = "Neon Postgres password for the app role"
  value       = neon_role.app.password
  sensitive   = true
}

output "postgres_url" {
  description = "Full POSTGRES_URL for .env"
  value       = "postgresql://${neon_role.app.name}:${neon_role.app.password}@${neon_project.main.database_host}/${neon_database.warehouse.name}?sslmode=require"
  sensitive   = true
}

output "r2_bucket_name" {
  description = "R2 bucket name (use as R2_BUCKET_RAW)"
  value       = cloudflare_r2_bucket.raw.name
}

output "r2_endpoint" {
  description = "R2 S3-compatible endpoint"
  value       = "https://${var.cloudflare_account_id}.r2.cloudflarestorage.com"
}
