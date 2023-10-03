# output "common" {
#   value = module.common
# }

# output "analytics_etl" {
#   value = module.analytics_etl
# }

# output "training" {
#   value = module.batch_training
# }

# output "inferencing" {
#   value = module.batch_inferencing
# }

# output "monitoring" {
#   value = try(module.monitoring, {})
# }

output "step_function_inputs" {
  value = {
    ssm_params       = values(aws_ssm_parameter.step_function_inputs).*.name
    training_inputs  = local.step_function_inputs.training_step_function_inputs
    inference_inputs = local.step_function_inputs.inferencing_step_function_inputs
  }
}
