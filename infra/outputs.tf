# infra/outputs.tf — actualiza el output de SQL
output "datalake_connection_string" {
  value     = azurerm_storage_account.datalake.primary_connection_string
  sensitive = true
}

output "postgresql_fqdn" {
  value = azurerm_postgresql_flexible_server.main.fqdn
}

output "postgresql_connection_string" {
  value     = "postgresql://${var.sql_admin_user}:${var.sql_admin_password}@${azurerm_postgresql_flexible_server.main.fqdn}/market_pulse_dw?sslmode=require"
  sensitive = true
}