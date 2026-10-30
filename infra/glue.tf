resource "aws_s3_object" "wheel" {
  bucket = aws_s3_bucket.data.id
  key    = "artifacts/stockstream-0.1.0-py3-none-any.whl"
  source = var.glue_wheel
  etag   = filemd5(var.glue_wheel)
}

locals {
  glue_jobs = {
    hourly-stats = { script = "hourly_stats.py", args = { "--LOOKBACK_HOURS" = "3" } }
    daily-stats  = { script = "daily_stats.py", args = { "--RUN_DATE" = "" } }
    compaction   = { script = "compaction.py", args = { "--RUN_DATE" = "" } }
  }
  glue_schedules = {
    hourly-stats = "cron(10 * * * ? *)"           # :10 past every hour
    daily-stats  = "cron(30 0 * * ? *)"           # 00:30 New York (timezone set on the schedule)
    compaction   = "cron(0 2 * * ? *)"            # 02:00 New York
  }
}

resource "aws_s3_object" "glue_script" {
  for_each = merge({ _common = { script = "_common.py" } }, local.glue_jobs)
  bucket   = aws_s3_bucket.data.id
  key      = "artifacts/glue/${each.value.script}"
  source   = "../glue_jobs/${each.value.script}"
  etag     = filemd5("../glue_jobs/${each.value.script}")
}

resource "aws_glue_job" "job" {
  for_each     = local.glue_jobs
  name         = "${var.name}-${each.key}"
  role_arn     = aws_iam_role.glue.arn
  max_capacity = 0.0625 # Python shell, 1/16 DPU: the cheapest Glue job size
  timeout      = 30

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.data.bucket}/artifacts/glue/${each.value.script}"
  }

  default_arguments = merge({
    "--DATA_BUCKET"               = aws_s3_bucket.data.bucket
    "--extra-py-files"            = "s3://${aws_s3_bucket.data.bucket}/${aws_s3_object.wheel.key},s3://${aws_s3_bucket.data.bucket}/artifacts/glue/_common.py"
    # Python shell 3.9 ships pandas 1.4.2; the stats code needs pandas 2.2 (the last line that supports 3.9).
    # tests run the same versions under Python 3.9 (see docs/fix-log.md, FIX-14).
    "--additional-python-modules" = "pandas==2.2.3,pyarrow==17.0.0,numpy==1.26.4,pyyaml==6.0.2,jsonschema==4.23.0"
    "library-set"                 = "analytics"
  }, each.value.args)
}

resource "aws_scheduler_schedule" "glue" {
  for_each                     = local.glue_jobs
  name                         = "${var.name}-${each.key}"
  schedule_expression          = local.glue_schedules[each.key]
  schedule_expression_timezone = "America/New_York"
  flexible_time_window { mode = "OFF" }

  target {
    arn      = "arn:aws:scheduler:::aws-sdk:glue:startJobRun"
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ JobName = aws_glue_job.job[each.key].name })
  }
}

resource "aws_glue_catalog_database" "db" {
  name = var.name
}

resource "aws_athena_workgroup" "wg" {
  name          = var.name
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    bytes_scanned_cutoff_per_query     = 1073741824 # 1 GB guardrail
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.bucket}/athena-results/"
    }
  }
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_data" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }
  statement {
    actions = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.data.arn}/gold/*", "${aws_s3_bucket.data.arn}/silver/*"]
  }
}

resource "aws_iam_role_policy" "glue_data" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data.json
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "scheduler" {
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "glue:StartJobRun", Resource = [for j in aws_glue_job.job : j.arn] }]
  })
}
