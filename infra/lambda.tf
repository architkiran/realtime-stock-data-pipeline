resource "aws_sqs_queue" "stream_dlq" {
  name                      = "${var.name}-stream-dlq"
  message_retention_seconds = 1209600 # 14 days
  sqs_managed_sse_enabled   = true
}

resource "aws_cloudwatch_log_group" "processor" {
  name              = "/aws/lambda/${var.name}-stream-processor"
  retention_in_days = 14
}

resource "aws_lambda_function" "processor" {
  function_name    = "${var.name}-stream-processor"
  role             = aws_iam_role.processor.arn
  runtime          = "python3.12"
  handler          = "stockstream.stream_processor.handler.lambda_handler"
  filename         = var.lambda_zip
  source_code_hash = filebase64sha256(var.lambda_zip)
  memory_size      = 1024
  timeout          = 120
  architectures    = ["arm64"]
  # pandas + pyarrow come from the AWS SDK for pandas managed layer (keeps the zip small).
  layers = [var.pandas_layer_arn]

  environment {
    variables = {
      DATA_BUCKET            = aws_s3_bucket.data.bucket
      LATE_WATERMARK_MINUTES = "120"
      LOG_LEVEL              = "INFO"
    }
  }
  depends_on = [aws_cloudwatch_log_group.processor]
}

resource "aws_lambda_event_source_mapping" "processor" {
  event_source_arn                   = aws_kinesis_stream.trades.arn
  function_name                      = aws_lambda_function.processor.arn
  starting_position                  = "LATEST"
  batch_size                         = 500
  maximum_batching_window_in_seconds = 5
  parallelization_factor             = 1
  maximum_retry_attempts             = 3
  bisect_batch_on_function_error     = true
  function_response_types            = ["ReportBatchItemFailures"]

  destination_config {
    on_failure { destination_arn = aws_sqs_queue.stream_dlq.arn }
  }
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "processor" {
  name               = "${var.name}-stream-processor"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "processor" {
  statement {
    actions = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetRecords",
    "kinesis:GetShardIterator", "kinesis:ListShards", "kinesis:SubscribeToShard"]
    resources = [aws_kinesis_stream.trades.arn]
  }
  statement {
    actions   = ["kms:Decrypt"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["kinesis.${var.region}.amazonaws.com"]
    }
  }
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.data.arn}/silver/*", "${aws_s3_bucket.data.arn}/quarantine/*"]
  }
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.stream_dlq.arn]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.processor.arn}:*"]
  }
}

resource "aws_iam_role_policy" "processor" {
  role   = aws_iam_role.processor.id
  policy = data.aws_iam_policy_document.processor.json
}
