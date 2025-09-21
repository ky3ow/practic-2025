terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.55"
    }
  }
}

provider "aws" {
  region = var.region
  profile = "ky3ow-iam"
}

provider "databricks" {
  alias = "mws"
  host   = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}

provider "databricks" {
  alias = "root"
  host   = databricks_mws_workspaces.this.workspace_url
}
