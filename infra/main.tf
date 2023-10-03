data "aws_caller_identity" "current" {}
locals {
  region_short = join("", split("-", var.region))
  name_prefix  = "${var.use_case_name}-${var.env}-${local.region_short}"

  inferencing_defaults = {
    evaluation_path = try(var.inferencing.evaluation_job_params.path, "model/inferencing_job/evaluation/inferencing_summary.py")
    gatekeeper_path = try(var.inferencing.gatekeeper_job_params.path, "model/inferencing_job/gatekeeper/gatekeeper.py")
  }

  training_defaults = {
    evaluation_path = try(var.training.evaluation_job_params.path, "model/training_job/evaluation/evaluation_summary.py")
    gatekeeper_path = try(var.training.gatekeeper_job_params.path, "model/training_job/gatekeeper/gatekeeper.py")
  }
  monitoring_defaults = {
    glue_data_quality_path              = try(var.monitoring.glue_data_quality_params.path, "model/common/monitoring/data_quality_eap.py")
    glue_feature_store_path             = try(var.monitoring.glue_feature_store_params.path, "model/training_job/monitoring/sample_feature_store.py")
    glue_inferencing_model_quality_path = try(var.monitoring.glue_inferencing_model_quality_params.path, "model/inferencing_job/monitoring/inference_model_quality_eap.py")
    glue_training_model_quality_path    = try(var.monitoring.glue_training_model_quality_params.path, "model/training_job/monitoring/training_model_quality_eap.py")
  }

  analytics_etl_defaults = {
    glue_config_paths = try(var.analytics_etl.glue_config_paths, ["analytics-etl/glue_jobs.config.json"])
  }

  mapping_json_path = "${path.root}/../${var.mapping_json_path}"
}

/*===============================
#            Common Infra
===============================*/


resource "aws_resourcegroups_group" "test" {
  name = "${local.name_prefix}-rg"

  resource_query {
    query = jsonencode({
      ResourceTypeFilters = ["AWS::AllSupported"]
      TagFilters = [
        {
          Key    = "project"
          Values = [var.use_case_name]
        },
        {
          Key    = "repo_url"
          Values = [var.repo_url]
        }
      ]
    })
  }
}

module "common" {
  source      = "./modules/common"
  name_prefix = local.name_prefix
}

/*===============================
#       Analytics-ETL
===============================*/

module "analytics_etl" {
  source = "./modules/analytics_etl"

  name_prefix                = local.name_prefix
  etl_glue_config_json_paths = try([for file in local.analytics_etl_defaults.glue_config_paths : "${path.root}/../${file}"], [])
  utils_path                 = "${path.root}/../${var.utils_path}"
  expected_dq_job_count      = var.analytics_etl.expected_dq_job_count
}


/*===============================
#       ML Batch Training
===============================*/

# TODO Add SSE to S3 buckets
module "batch_training" {
  source = "github.com/MSIL-Analytics-ACE/terraform-awsproserv-dapm-framework//constructs/batch_training?ref=main"

  name_prefix   = local.name_prefix
  use_case_name = var.use_case_name
  environment   = var.env

  # Glue Job Parameters
  utils_path = "${path.root}/../${var.utils_path}"

  # Customizable Glue jobs
  evaluation_job_params = merge(var.training.evaluation_job_params, {
    path = "${path.root}/../${local.training_defaults.evaluation_path}"
  })
  gatekeeper_job_params = merge(var.training.gatekeeper_job_params, {
    path = "${path.root}/../${local.training_defaults.gatekeeper_path}"
  })

  event_bus_name           = module.common.event_bus.name
  model_package_group_name = module.common.sagemaker_mpg.id

  sagemaker_processing_job_execution_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.sagemaker_processing_job_execution_role_name}"
}

/*===============================
#       ML Batch Inferencing
===============================*/

module "batch_inferencing" {
  source = "github.com/MSIL-Analytics-ACE/terraform-awsproserv-dapm-framework//constructs/batch_inferencing?ref=main"

  name_prefix   = local.name_prefix
  use_case_name = var.use_case_name
  environment   = var.env

  # Glue Job Parameters
  utils_path = "${path.root}/../${var.utils_path}"

  # Customizable Glue jobs
  evaluation_job_params = merge(var.inferencing.evaluation_job_params, {
    path = "${path.root}/../${local.inferencing_defaults.evaluation_path}"
  })
  gatekeeper_job_params = merge(var.inferencing.gatekeeper_job_params, {
    path = "${path.root}/../${local.inferencing_defaults.gatekeeper_path}"
  })

  sagemaker_processing_job_execution_role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.sagemaker_processing_job_execution_role_name}"

  batch_vpc = var.inferencing.batch_vpc
}

/*===============================
#       ML Monitoring and Cross account operations
===============================*/

module "monitoring" {
  source = "github.com/MSIL-Analytics-ACE/terraform-awsproserv-dapm-framework//constructs/monitoring?ref=main"

  count = var.enable_monitoring ? 1 : 0

  name_prefix = local.name_prefix
  utils_path  = "${path.root}/../${var.utils_path}"

  glue_data_quality = merge(var.monitoring.glue_data_quality_params, {
    path = "${path.root}/../${local.monitoring_defaults.glue_data_quality_path}"
  })

  glue_feature_store = merge(var.monitoring.glue_feature_store_params, {
    path = "${path.root}/../${local.monitoring_defaults.glue_feature_store_path}"
  })

  glue_inferencing_model_quality = merge(var.monitoring.glue_inferencing_model_quality_params, {
    path          = "${path.root}/../${local.monitoring_defaults.glue_inferencing_model_quality_path}"
    use_case_name = var.use_case_name
  })

  glue_training_model_quality = merge(var.monitoring.glue_training_model_quality_params, {
    path = "${path.root}/../${local.monitoring_defaults.glue_training_model_quality_path}"
  })

  training_internal_bucket_name  = module.batch_training.training_contruct.s3_buckets["internal"].s3_bucket_id
  inferencing_shared_bucket_name = module.batch_inferencing.inferencing_contruct.s3_buckets["shared"].s3_bucket_id
  monitoring_athena_db           = module.analytics_etl.athena_db["monitoring"].id
  eap_dq_bucket_name             = var.eap_dq_bucket_name
  event_bus_name                 = module.common.event_bus.name
  training_events                = module.batch_training.exposed_events
}

/*===============================
#       SFN Input Generation
===============================*/

locals {
  step_function_inputs = {
    training_step_function_inputs = {
      repository                             = var.repo_url
      use_case_name                          = var.use_case_name
      region                                 = var.region
      step_function_arn                      = module.batch_training.training_contruct.step_function.state_machine_arn
      train_statetable_name                  = module.batch_training.training_contruct.ddb_tables["state_table"].dynamodb_table_id
      train_inputtable_name                  = module.batch_training.training_contruct.ddb_tables["input_table"].dynamodb_table_id
      train_metatable_name                   = module.batch_training.training_contruct.ddb_tables["meta_table"].dynamodb_table_id
      s3_bucket_name_analytics_etl           = module.analytics_etl.s3.id
      s3_bucket_name_internal                = module.batch_training.training_contruct.s3_buckets["internal"].s3_bucket_id
      s3_bucket_name_shared                  = module.batch_training.training_contruct.s3_buckets["shared"].s3_bucket_id
      mapping_json_S3_path                   = "s3://${module.batch_training.training_contruct.s3_buckets["internal"].s3_bucket_id}/mapping_json/mapping_json.json"
      aws_batch_job_name                     = "${local.name_prefix}-job"
      aws_batch_job_queue                    = module.batch_training.training_contruct.batch.batch.job_queues.low_priority.name
      ssm_training_complete_status           = module.batch_training.training_contruct.ssm_params["context_complete_status"]
      training_event_bus_name                = module.common.event_bus.name
      sns_topic_arn                          = module.common.sns_topic.arn
      dq_athena_db                           = module.analytics_etl.athena_db["data_quality"].id
      model_package_group_name               = module.common.sagemaker_mpg.id
      athenadb_name                          = module.analytics_etl.athena_db["operations"].id
      dq_table                               = "dqresults"
      athena_pred_or_eval_table_name         = "evaluation"
      athenadb_debug_table_name              = "debug"
      athenadb_evaluation_summary_table_name = "model_eval_summary"
      athenadb_metadata_table_name           = "metatable"
    }

    inferencing_step_function_inputs = {
      use_case_name                           = var.use_case_name
      region                                  = var.region
      step_function_arn                       = module.batch_inferencing.inferencing_contruct.step_function.state_machine_arn
      aws_batch_job_name                      = "${local.name_prefix}-job"
      aws_batch_job_queue                     = module.batch_inferencing.inferencing_contruct.batch.batch.job_queues.low_priority.name
      s3_bucket_name_analytics_etl            = module.analytics_etl.s3.id
      s3_bucket_name_internal                 = module.batch_inferencing.inferencing_contruct.s3_buckets["internal"].s3_bucket_id
      s3_bucket_name_shared                   = module.batch_inferencing.inferencing_contruct.s3_buckets["shared"].s3_bucket_id
      inference_statetable_name               = module.batch_inferencing.inferencing_contruct.ddb_tables["state_table"].dynamodb_table_id
      inference_inputtable_name               = module.batch_inferencing.inferencing_contruct.ddb_tables["input_table"].dynamodb_table_id
      inference_metatable_name                = module.batch_inferencing.inferencing_contruct.ddb_tables["meta_table"].dynamodb_table_id
      mapping_json_S3_inference_path          = "s3://${module.batch_training.training_contruct.s3_buckets["internal"].s3_bucket_id}/mapping_json/mapping_json.json"
      ssm_inferencing_complete_status         = module.batch_inferencing.inferencing_contruct.ssm_params["context_complete_status"]
      training_event_bus_name                 = module.common.event_bus.name
      sns_topic_arn                           = module.common.sns_topic.arn
      dq_athena_db                            = module.analytics_etl.athena_db["data_quality"].id
      ssm_winning_algo_name                   = module.batch_training.ssm_model_params["winner_algorithm"].name
      ssm_approved_model_prefix_path          = module.batch_training.ssm_model_params["approved_model_prefix_path"].name
      training_athenadb_name                  = module.analytics_etl.athena_db["operations"].id
      inference_athenadb_name                 = module.analytics_etl.athena_db["operations"].id
      dq_table                                = "dqresults"
      athena_pred_or_eval_table_name          = "evaluation"
      training_athenadb_metadata_table_name   = "metatable"
      inference_athenadb_debug_table_name     = "debug_inference"
      inference_athenadb_metadata_table_name  = "meta_inference"
      inference_athenadb_inference_table_name = "inference"
    }
  }
}

resource "aws_ssm_parameter" "step_function_inputs" {

  for_each = local.step_function_inputs

  name           = "/${local.name_prefix}-dapf-ssm/${each.key}"
  type           = "String"
  insecure_value = jsonencode(each.value)

  overwrite = true
}

resource "aws_s3_object" "upload_mapping_json" {

  bucket = module.batch_training.training_contruct.s3_buckets["internal"].s3_bucket_id
  key    = "mapping_json/mapping_json.json"

  source      = local.mapping_json_path
  source_hash = filemd5(local.mapping_json_path)
}
