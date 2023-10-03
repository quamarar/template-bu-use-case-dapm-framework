/*===============================
#              KMS
===============================*/

output "kms" {
  value = module.kms_default
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
