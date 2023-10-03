provider "aws" {
  region = var.region

  assume_role {
    role_arn = "arn:aws:iam::${var.account_number}:role/Jenkins-AssumeRole-ProServe-CrossAccount-Role"
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

  backend "s3" {
    region         = "ap-south-1"
    key            = "quality-dcp/development/terraform.tfstate" # These needs to be set by pipeline
    bucket         = "eap-central-tf-state-bucket"
    dynamodb_table = "eap-central-tf-state-bucket"
    encrypt        = true
    kms_key_id     = "arn:aws:kms:ap-south-1:209991378390:key/f9769abe-9eb0-429b-8e8f-6fd7efc7c6bb"
    role_arn       = "arn:aws:iam::209991378390:role/Jenkins-AssumeRole-ProServe-CrossAccount-Role"
  }
}

