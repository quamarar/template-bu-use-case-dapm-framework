variable "name_prefix" {

  type        = string
  description = "Prefix to be appended to all resource names"
}

variable "use_case_name" {
  type        = string
  description = "use case name identifier"
}

variable "athena_databases" {
  type        = any
  description = "athena db names"
  default     = {}
}

variable "etl_glue_config_json_paths" {
  type        = list(string)
  description = "Path of glue json which contains config"
}

variable "etl_ssm_params" {
  type        = any
  description = "provide list of ssm params to create"
  default     = {}
}

variable "utils_path" {
  type        = string
  description = "paths to utilities"
}

variable "expected_dq_job_count" {
  type        = number
  description = "Expected number of ETL dq jobs which are matched for email functionality"
}

variable "eap_dq_bucket_name" {
  type        = string
  description = "S3 bucket name from EAP account"
  default     = "random"
}