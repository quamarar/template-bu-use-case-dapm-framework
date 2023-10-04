locals {
  context = "analytics-etl"

  framework_ssm_params = merge(
    {
      expected_dq_job_cnt = {
        description = "Expected job counts"
        value       = var.expected_dq_job_count
      }
  }, var.etl_ssm_params)

  # TODO: Create glue catalog instead in common
  athena_databases = merge({
    data_quality = {
      bucket      = aws_s3_bucket.analytics_bucket.id
      description = "data quality athena db database"
    }
    monitoring = {
      bucket      = aws_s3_bucket.analytics_bucket.id
      description = "monitoring athendb database"
    }
    operations = {
      bucket      = aws_s3_bucket.analytics_bucket.id
      description = "operations athendb database"
    }
  }, var.athena_databases)
}



/* -------------------------------------------------------------------------- */
/*                              Athena Databases                              */
/* -------------------------------------------------------------------------- */

resource "aws_s3_bucket" "analytics_bucket" {
  bucket        = replace("${var.name_prefix}-${local.context}", "_", "-")
  force_destroy = true

     server_side_encryption_configuration {
     rule {
       apply_server_side_encryption_by_default {
         kms_master_key_id = var.kms_master_key_id
         sse_algorithm     = "aws:kms"
       }
     }
   }
 }

resource "aws_s3_bucket_public_access_block" "analytics_bucket" {
  bucket = aws_s3_bucket.analytics_bucket.id
  block_public_acls   = true
  block_public_policy = true
  ignore_public_acls  = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "versioning_example" {
  bucket = aws_s3_bucket.analytics_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_athena_database" "athena_db" {

  for_each = local.athena_databases

  name          = replace("${var.name_prefix}-${each.key}", "-", "_")
  bucket        = each.value.bucket
  force_destroy = try(each.value.force_destroy, true)
  properties    = try(each.value.properties, null)
  comment       = try(each.value.description, null)

 }
 


resource "aws_ssm_parameter" "ssm_params" {

  for_each = local.framework_ssm_params

  name           = "/${var.name_prefix}-dapf-ssm/${local.context}/${each.key}"
  description    = each.value.description
  type           = "String"
  insecure_value = try(each.value.value, "default")
}
