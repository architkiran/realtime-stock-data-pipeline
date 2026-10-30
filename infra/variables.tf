variable "region" {
  type    = string
  default = "us-east-1"
}

variable "name" {
  description = "Prefix for every resource name"
  type        = string
  default     = "stockstream"
}

variable "alert_email" {
  description = "Email that receives CloudWatch alarm and budget notifications"
  type        = string
}

variable "monthly_budget_usd" {
  type    = number
  default = 20
}

variable "sources" {
  description = "Comma-separated live sources the producer streams"
  type        = string
  default     = "finnhub,coinbase"
}

variable "producer_image" {
  description = "ECR image URI for the producer (make push-producer prints it)"
  type        = string
  default     = ""
}

variable "producer_desired_count" {
  description = "0 = producer off (no Fargate cost), 1 = streaming"
  type        = number
  default     = 0
}

variable "lambda_zip" {
  description = "Path to the stream processor deployment zip built by `make build-lambda`"
  type        = string
  default     = "../build/stream_processor.zip"
}

variable "glue_wheel" {
  description = "Path to the stockstream wheel built by `make build-wheel`"
  type        = string
  default     = "../build/stockstream-0.1.0-py3-none-any.whl"
}

variable "pandas_layer_arn" {
  description = "AWS SDK for pandas managed layer for python3.12 arm64. Check the current version at https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html"
  type        = string
  default     = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312-Arm64:15"
}
