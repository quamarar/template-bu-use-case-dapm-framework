/*===============================
#              KMS
===============================*/

output "kms" {
  value = module.kms_default
}

output "key_id" {
 value = module.kms_default.key_id
}

output "key_arn" {
 value = module.kms_default.key_arn
}
/*===============================
#              S3
===============================*/

output "sagemaker_mpg" {
  value = aws_sagemaker_model_package_group.mpg
}

output "event_bus" {
  value = aws_cloudwatch_event_bus.messenger
}

output "sns_topic" {
  value = aws_sns_topic.framework_sns
}
