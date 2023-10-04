terraform {
  required_providers {
    utils = {
      source = "cloudposse/utils"
    }
  }
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
data "utils_deep_merge_json" "merged_json" {
  input          = local.redered_files
  append_list    = true
  deep_copy_list = true
}

locals {

  # Note: Dependency references wont work with rendering below template.
  redered_files = [for json in var.etl_glue_config_json_paths :
    templatefile(json,
      {
        tfvars_analytics_internal_bucket     = replace("${var.name_prefix}-${local.context}", "_", "-")
        tfvars_region                        = data.aws_region.current.name
        tfvars_account_number                = data.aws_caller_identity.current.account_id
        tfvars_common_sns_topic              = "arn:aws:sns:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${var.name_prefix}-dapf-notifier"
        tfvars_workflow_count_ssm_param_name = "/${var.name_prefix}-dapf-ssm/${local.context}/expected_dq_job_cnt"
        tfvars_dq_athena_db                  = replace("${var.name_prefix}-data_quality", "-", "_")
        tfvars_dq_s3_bucket                  = replace("${var.name_prefix}-${local.context}", "_", "-")
        tfvars_eap_dq_bucket_name            = var.eap_dq_bucket_name
        tfvars_use_case_name                 = var.use_case_name
    })
  ]

  merged_glue_jsons = jsondecode(data.utils_deep_merge_json.merged_json.output)
}


/* -------------------------------------------------------------------------- */
/*                             Glue Job creation                              */
/* -------------------------------------------------------------------------- */

module "glue_jobs" {
  source = "git::https://github.com/quamarar/terraform-common-modules//terraform-aws-glue-job?ref=master"

  for_each = try(local.merged_glue_jsons.jobs, {})

  job_name        = replace(lower("${var.name_prefix}-${local.context}-${each.key}"), "_", "-")
  job_description = try(each.value.description, null)
  glue_version    = try(each.value.glue_version, null)
  role_arn        = each.value.role_arn

  # Specific to glueetl
  number_of_workers = try(each.value.number_of_workers, null)
  worker_type       = try(each.value.worker_type, null)

  # This is to ensure there are no concurrent runs
  max_capacity = try(each.value.max_capacity, null)
  max_retries  = try(each.value.max_retries, 0)
  timeout      = try(each.value.timeout, 2880)

  connections = try(each.value.connections, null)

  command = {
    name            = each.value.type
    script_location = "s3://${aws_s3_bucket.analytics_bucket.id}/src/python/${local.context}/${each.value.file_name}"
    python_version  = try(each.value.python_version, 3.9)
  }

  execution_property = try(each.value.execution_property, {
    max_concurrent_runs = 1
  })

  default_arguments = each.value.default_arguments
}

resource "aws_s3_object" "upload_utils" {
  for_each = fileset(var.utils_path, "**")

  bucket = aws_s3_bucket.analytics_bucket.id
  key    = "src/python/utils/${each.value}"

  source      = "${var.utils_path}/${each.value}"
  source_hash = filemd5("${var.utils_path}/${each.value}")

  # lifecycle {
  #   ignore_changes = [
  #     tags,
  #     tags_all
  #   ]
  # }
}

resource "aws_s3_object" "upload_ext_scripts" {
  for_each = merge([
    for file_paths in var.etl_glue_config_json_paths : { for file in fileset(dirname(file_paths), "**.py") : file => {
      fileName = file
      filePath = dirname(file_paths)
      }
    }
  ]...)

  bucket = aws_s3_bucket.analytics_bucket.id
  key    = "src/python/${local.context}/${each.value.fileName}"

  source      = "${each.value.filePath}/${each.value.fileName}"
  source_hash = filemd5("${each.value.filePath}/${each.value.fileName}")

  # lifecycle {
  #   ignore_changes = [
  #     tags,
  #     tags_all
  #   ]
  # }
}

module "glue_wfs" {
  source = "git::https://github.com/quamarar/terraform-common-modules//terraform-aws-glue-workflow?ref=master"

  for_each = try(local.merged_glue_jsons.workflows, {})

  workflow_name          = replace(lower("${var.name_prefix}-${local.context}-${each.key}"), "_", "-")
  workflow_description   = each.value.description
  default_run_properties = each.value.default_run_properties
  max_concurrent_runs    = each.value.max_concurrent_runs
  triggers = { for tg_key, tg_value in each.value.triggers :
    replace(lower("${var.name_prefix}-${local.context}-${tg_key}"), "_", "-") => merge(tg_value, {
      actions = [for action in tg_value.actions :
        merge(action, { job_name = try(module.glue_jobs[action.job_name].name, action.job_name) })
      ]
      }, try(
      {
        predicate = {
          logical = tg_value.predicate.logical
          conditions = [for condition in tg_value.predicate.conditions :
            merge(condition, { job_name = try(module.glue_jobs[condition.job_name].name, condition.job_name) })
          ]
        }
      },
      {})
    )
  }
}
