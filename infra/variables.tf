/*===============================
#       Common Details
===============================*/

variable "use_case_name" {
  type        = string
  description = "use case name identifier"
}

variable "env" {
  type        = string
  description = "Stage / Environment identifier"
}

variable "region" {
  type        = string
  description = "region to deploy resources"
}

variable "repo_url" {
  type        = string
  description = "Repository URL"
}

variable "account_number" {
  type        = number
  description = "target account number"
}

/*===============================
#   Batch Training Variables
===============================*/

variable "utils_path" {
  type        = string
  description = "paths to utilities"
  default     = "utils"
}

variable "training" {
  type = object({
    evaluation_job_params = optional(any)
    gatekeeper_job_params = optional(any)
  })

  default = {
    "evaluation_job_params" : {
      "path" : "model/training_job/evaluation/evaluation_summary.py"
    },
    "gatekeeper_job_params" : {
      "path" : "model/training_job/gatekeeper/gatekeeper.py"
    }
  }
}

variable "inferencing" {
  type = object({
    evaluation_job_params = optional(any)
    gatekeeper_job_params = optional(any)
    batch_vpc = optional(object({
      vpc_id     = string
      subnet_ids = list(string)
    }))
  })

  validation {
    error_message = "Must contain VPC."
    condition     = var.inferencing.batch_vpc != {}
  }

  default = {
    "evaluation_job_params" : {
      "path" : "model/inferencing_job/evaluation/inferencing_summary.py"
    },
    "gatekeeper_job_params" : {
      "path" : "model/inferencing_job/gatekeeper/gatekeeper.py"
    }
  }
}

variable "monitoring" {
  type = object({
    glue_data_quality_params              = optional(any)
    glue_feature_store_params             = optional(any)
    glue_inferencing_model_quality_params = optional(any)
    glue_training_model_quality_params    = optional(any)
  })

  default = {}
}

variable "analytics_etl" {
  type = object({
    glue_config_paths     = optional(any)
    expected_dq_job_count = number
  })

  default = {
    glue_config_path      = null
    expected_dq_job_count = null
  }

  validation {
    condition     = var.analytics_etl.expected_dq_job_count != null
    error_message = "Expected Job count required."
  }
}

variable "mapping_json_path" {
  type        = string
  description = "Provide path relative to root for mapping json"
}

variable "sagemaker_processing_job_execution_role_name" {
  type        = string
  description = "To be removed"
}

variable "enable_monitoring" {
  type        = bool
  description = "S3 bucket name from EAP account"
  default     = false
}

variable "eap_dq_bucket_name" {
  type        = string
  description = "S3 bucket name from EAP account"
  default     = "random"
}
