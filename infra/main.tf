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

# ── PostgreSQL Flexible Server (free tier) ────────────────────
resource "azurerm_postgresql_flexible_server" "main" {
  name                   = "psql-market-pulse-001"
  resource_group_name    = azurerm_resource_group.main.name
  location               = "eastus2"
  version                = "16"
  administrator_login    = var.sql_admin_user
  administrator_password = var.sql_admin_password

  sku_name   = "B_Standard_B1ms"  # free tier elegible
  storage_mb = 32768              # 32GB — mínimo free tier

  backup_retention_days        = 7
  geo_redundant_backup_enabled = false

  # free tier: solo disponible una vez por suscripción
  create_mode = "Default"

  tags = { project = "market-pulse" }
}

resource "azurerm_postgresql_flexible_server_database" "warehouse" {
  name      = "market_pulse_dw"
  server_id = azurerm_postgresql_flexible_server.main.id
  collation = "en_US.utf8"
  charset   = "utf8"
}

# Firewall: acceso desde tu IP local
resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_local" {
  name             = "AllowLocalDev"
  server_id        = azurerm_postgresql_flexible_server.main.id
  start_ip_address = var.local_ip
  end_ip_address   = var.local_ip
}

# Firewall: acceso desde servicios Azure (GitHub Actions, etc.)
resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_postgresql_flexible_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}