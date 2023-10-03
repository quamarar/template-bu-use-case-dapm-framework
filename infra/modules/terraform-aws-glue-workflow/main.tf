resource "aws_glue_workflow" "this" {

  name                   = var.workflow_name
  description            = var.workflow_description
  default_run_properties = var.default_run_properties
  max_concurrent_runs    = var.max_concurrent_runs

  tags = var.tags
}

resource "aws_glue_trigger" "this" {
  for_each = var.triggers

  name              = each.key
  description       = try(each.value.description, null)
  workflow_name     = var.workflow_name
  type              = each.value.type
  schedule          = try(each.value.schedule, null)
  enabled           = try(each.value.enabled, true)
  start_on_creation = each.value.type == "ON_DEMAND" ? false : try(each.value.start_on_creation, false)

  dynamic "predicate" {
    for_each = try(each.value.predicate, null) != null ? [true] : []

    content {
      logical = try(each.value.predicate.logical, null)

      dynamic "conditions" {
        for_each = try(each.value.predicate.conditions, [])

        content {
          job_name         = try(conditions.value.job_name, null)
          state            = try(conditions.value.state, null)
          crawler_name     = try(conditions.value.crawler_name, null)
          crawl_state      = try(conditions.value.crawl_state, null)
          logical_operator = try(conditions.value.logical_operator, null)
        }
      }
    }
  }

  dynamic "actions" {
    for_each = try(each.value.actions, [])

    content {
      job_name               = try(actions.value.job_name, null)
      crawler_name           = try(actions.value.crawler_name, null)
      arguments              = try(actions.value.arguments, null)
      security_configuration = try(actions.value.security_configuration, null)
      timeout                = try(actions.value.timeout, null)

      # dynamic "notification_property" {
      #   for_each = try(actions.value.notification_property, null) != null ? [true] : []
      #   content {
      #     notify_delay_after = try(actions.value.notification_property.NotifyDelayAfter, null)
      #   }
      # }
    }
  }

  dynamic "event_batching_condition" {
    for_each = try(each.value.event_batching_condition, null) != null ? [true] : []

    content {
      batch_size   = try(each.value.event_batching_condition.batch_size, null)
      batch_window = try(each.value.event_batching_condition.batch_window, null)
    }
  }

  tags = var.tags
}
