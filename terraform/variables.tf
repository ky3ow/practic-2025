variable "databricks_account_id" {
  default = "d141c655-acae-4662-9c74-db4fe75fc926"
}

variable "tags" {
  default = {}
}

variable "cidr_block" {
  default = "10.4.0.0/16"
}

variable "region" {
  default = "eu-west-1"
}

variable "admins" {
  default = ["vova2341591@gmail.com"]
}

resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

locals {
  prefix = "demo${random_string.naming.result}"
}
