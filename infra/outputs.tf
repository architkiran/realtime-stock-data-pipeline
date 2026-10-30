output "bucket" { value = aws_s3_bucket.data.bucket }
output "stream_name" { value = aws_kinesis_stream.trades.name }
output "ecr_repository" { value = aws_ecr_repository.producer.repository_url }
output "athena_workgroup" { value = aws_athena_workgroup.wg.name }
output "api_secret_name" { value = aws_secretsmanager_secret.api_keys.name }
