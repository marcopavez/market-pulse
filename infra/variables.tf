# infra/variables.tf
variable "resource_group_name" { default = "rg-market-pulse" }
variable "location"            { default = "eastus" }

variable "storage_account_name" {
  description = "Globally unique, 3-24 chars, lowercase alphanumeric"
  default     = "stmarketpulse001"
}

variable "sql_server_name" {
  description = "Globally unique"
  default     = "sql-market-pulse-002"
}

variable "sql_admin_user"     { sensitive = true }
variable "sql_admin_password" { sensitive = true }
variable "local_ip"           { description = "Your public IP for dev access" }