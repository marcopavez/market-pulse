# infra/outputs.tf
output "datalake_connection_string" {
  value     = azurerm_storage_account.datalake.primary_connection_string
  sensitive = true
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.main.fully_qualified_domain_name
}