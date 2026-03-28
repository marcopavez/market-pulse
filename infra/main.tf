# infra/main.tf
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }
  # Store state in Azure Blob (free within storage account)
  backend "azurerm" {
    resource_group_name  = "rg-market-pulse"
    storage_account_name = "stmarketpulsetf"
    container_name       = "tfstate"
    key                  = "prod.terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

# ── Resource Group ────────────────────────────────────────────
resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
}

# ── Storage Account (Data Lake Gen2) ─────────────────────────
resource "azurerm_storage_account" "datalake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"          # cheapest, fine for dev
  is_hns_enabled           = true           # enables Data Lake Gen2
}

resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw"
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "processed" {
  name               = "processed"
  storage_account_id = azurerm_storage_account.datalake.id
}

# ── Azure SQL Database (Basic ~$5/mes) ────────────────────────
resource "azurerm_mssql_server" "main" {
  name                         = var.sql_server_name
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_user
  administrator_login_password = var.sql_admin_password
}

resource "azurerm_mssql_database" "warehouse" {
  name      = "market-pulse-dw"
  server_id = azurerm_mssql_server.main.id
  sku_name  = "Basic"   # 5 DTUs, ~$5/mes
  max_size_gb = 2
}

# Firewall: permite IPs de GitHub Actions (rangos dinámicos)
# + tu IP local para desarrollo
resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_mssql_firewall_rule" "allow_local" {
  name             = "AllowLocalDev"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = var.local_ip
  end_ip_address   = var.local_ip
}