provider "aws" {
  region = var.region

  assume_role {
    role_arn = "arn:aws:iam::${var.account_number}:role/Cross-Account-role"
  }

  forbidden_account_ids = [
    "209991378390"
  ]

  default_tags {
    tags = {
      environment  = var.env
      deployed_by  = "TFProviders"
      developed_by = "AWSProserve"
      project      = var.use_case_name
      repo_url     = var.repo_url
    }
  }
}

terraform {
  required_version = ">= 0.15, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 4.61.0"
    }
    awscc = {
      source  = "hashicorp/awscc"
      version = ">= 0.49.0"
    }
  }
}

