resource "aws_sns_topic" "alerts" {
  name = "${var.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

locals {
  alarms = {
    iterator-age = {
      namespace = "AWS/Lambda", metric = "IteratorAge", stat = "Maximum", threshold = 60000, periods = 3
      dims      = { FunctionName = aws_lambda_function.processor.function_name }
      desc      = "Lambda is falling behind the stream by more than 60 s"
    }
    lambda-errors = {
      namespace = "AWS/Lambda", metric = "Errors", stat = "Sum", threshold = 0, periods = 5
      dims      = { FunctionName = aws_lambda_function.processor.function_name }
      desc      = "Stream processor is throwing errors"
    }
    dlq-depth = {
      namespace = "AWS/SQS", metric = "ApproximateNumberOfMessagesVisible", stat = "Maximum", threshold = 0, periods = 1
      dims      = { QueueName = aws_sqs_queue.stream_dlq.name }
      desc      = "Batches gave up after retries and went to the DLQ"
    }
    crypto-freshness = {
      namespace = "StockStream", metric = "LastTradeAgeSeconds", stat = "Maximum", threshold = 300, periods = 5
      dims      = { Source = "coinbase" }
      desc      = "No crypto trade for 5 minutes: the producer or the websocket is down"
    }
    write-throttled = {
      namespace = "AWS/Kinesis", metric = "WriteProvisionedThroughputExceeded", stat = "Sum", threshold = 0, periods = 5
      dims      = { StreamName = aws_kinesis_stream.trades.name }
      desc      = "Producer is being throttled: add a shard"
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "alarm" {
  for_each            = local.alarms
  alarm_name          = "${var.name}-${each.key}"
  alarm_description   = each.value.desc
  namespace           = each.value.namespace
  metric_name         = each.value.metric
  statistic           = each.value.stat
  dimensions          = each.value.dims
  period              = 60
  evaluation_periods  = each.value.periods
  threshold           = each.value.threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# Glue job failures arrive as EventBridge events, not metrics.
resource "aws_cloudwatch_event_rule" "glue_failed" {
  name = "${var.name}-glue-failed"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail      = { state = ["FAILED", "TIMEOUT"], jobName = [for j in aws_glue_job.job : j.name] }
  })
}

resource "aws_cloudwatch_event_target" "glue_failed" {
  rule = aws_cloudwatch_event_rule.glue_failed.name
  arn  = aws_sns_topic.alerts.arn
}

# A topic policy REPLACES the default one, so every publisher must be listed:
# EventBridge (Glue failure events) and CloudWatch (alarms). Budgets emails directly.
data "aws_iam_policy_document" "sns_events" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "cloudwatch.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.me.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.sns_events.json
}

resource "aws_budgets_budget" "monthly" {
  name         = "${var.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 50
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = var.name
  dashboard_body = jsonencode({
    widgets = [
      { type = "metric", x = 0, y = 0, width = 12, height = 6, properties = {
        title = "Records in", region = var.region, stat = "Sum", period = 60,
        metrics = [["AWS/Kinesis", "IncomingRecords", "StreamName", aws_kinesis_stream.trades.name]] } },
      { type = "metric", x = 12, y = 0, width = 12, height = 6, properties = {
        title = "Lambda iterator age (ms)", region = var.region, stat = "Maximum", period = 60,
        metrics = [["AWS/Lambda", "IteratorAge", "FunctionName", aws_lambda_function.processor.function_name]] } },
      { type = "metric", x = 0, y = 6, width = 12, height = 6, properties = {
        title = "Lambda errors and duration", region = var.region, period = 60,
        metrics = [["AWS/Lambda", "Errors", "FunctionName", aws_lambda_function.processor.function_name, { stat = "Sum" }],
        [".", "Duration", ".", ".", { stat = "p95", yAxis = "right" }]] } },
      { type = "metric", x = 12, y = 6, width = 12, height = 6, properties = {
        title = "DLQ depth", region = var.region, stat = "Maximum", period = 60,
        metrics = [["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.stream_dlq.name]] } },
    ]
  })
}
