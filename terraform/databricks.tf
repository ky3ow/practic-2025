data "aws_caller_identity" "current" {}

locals {
  uc_iam_role = "${local.prefix}-uc-access"
}

resource "databricks_group" "admin" {
  provider     = databricks.mws
  display_name = "Account Admin"
}

data "databricks_user" "admins" {
  provider  = databricks.mws
  for_each  = toset(var.admins)
  user_name = each.value
}

resource "databricks_group_member" "admin_group_members" {
  provider  = databricks.mws
  for_each  = toset(var.admins)
  group_id  = databricks_group.admin.id
  member_id = data.databricks_user.admins[each.value].id
}

resource "databricks_storage_credential" "external" {
  provider = databricks.root
  name     = "${local.prefix}-external-access"
  //cannot reference aws_iam_role directly, as it will create circular dependency
  aws_iam_role {
    role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.uc_iam_role}"
  }
  comment = "Managed by TF"
}

resource "databricks_grants" "external_creds" {
  provider           = databricks.root
  storage_credential = databricks_storage_credential.external.id

  grant {
    principal  = databricks_group.admin.display_name
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "aws_s3_bucket" "external" {
  bucket = "${local.prefix}-external"
  // destroy all objects with bucket destroy
  force_destroy = true
  tags = merge(var.tags, {
    Name = "${local.prefix}-external"
  })
}

resource "aws_s3_bucket_versioning" "external_versioning" {
  bucket = aws_s3_bucket.external.id
  versioning_configuration {
    status = "Disabled"
  }
}

data "databricks_aws_unity_catalog_assume_role_policy" "this" {
  aws_account_id = data.aws_caller_identity.current.account_id
  role_name      = local.uc_iam_role
  external_id    = databricks_storage_credential.external.aws_iam_role[0].external_id
}

data "databricks_aws_unity_catalog_policy" "this" {
  aws_account_id = data.aws_caller_identity.current.account_id
  bucket_name    = aws_s3_bucket.external.id
  role_name      = local.uc_iam_role
}

resource "aws_iam_policy" "external_data_access" {
  policy = data.databricks_aws_unity_catalog_policy.this.json
  tags = merge(var.tags, {
    Name = "${local.prefix}-unity-catalog external access IAM policy"
  })
}

resource "aws_iam_role" "external_data_access" {
  name               = local.uc_iam_role
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.this.json
  tags = merge(var.tags, {
    Name = "${local.prefix}-unity-catalog external access IAM role"
  })
}

resource "aws_iam_role_policy_attachment" "external_data_access" {
  role       = aws_iam_role.external_data_access.name
  policy_arn = aws_iam_policy.external_data_access.arn
}

resource "databricks_external_location" "some" {
  provider        = databricks.root
  name            = "external"
  url             = "s3://${aws_s3_bucket.external.id}/some"
  credential_name = databricks_storage_credential.external.id
  comment         = "Managed by TF"
}

resource "databricks_grants" "some" {
  provider          = databricks.root
  external_location = databricks_external_location.some.id
  grant {
    principal  = databricks_group.admin.display_name
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "databricks_catalog" "sandbox" {
  provider     = databricks.root
  storage_root = "s3://${aws_s3_bucket.external.id}/some"
  name         = "sandbox"
  comment      = "this catalog is managed by terraform"
  properties = {
    purpose = "testing"
  }
}

resource "databricks_grants" "sandbox" {
  provider = databricks.root
  catalog  = databricks_catalog.sandbox.id
  grant {
    principal  = databricks_group.admin.display_name
    privileges = ["ALL_PRIVILEGES", "MANAGE"]
  }
}

resource "databricks_schema" "things" {
  provider     = databricks.root
  catalog_name = databricks_catalog.sandbox.id
  name         = "things"
  comment      = "this database is managed by terraform"
  properties = {
    kind = "various"
  }
}
