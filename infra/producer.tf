# The producer is a long-lived websocket client, so it runs as one small Fargate task.
# desired_count = 0 keeps it (and its cost) off until you are ready to stream.

resource "aws_secretsmanager_secret" "api_keys" {
  name = "${var.name}/api-keys"
  # Set the value once, outside Terraform, so keys never land in state:
  # aws secretsmanager put-secret-value --secret-id stockstream/api-keys \
  #   --secret-string '{"finnhub_key":"...","alpaca_key":"...","alpaca_secret":"..."}'
}

resource "aws_ecr_repository" "producer" {
  name                 = "${var.name}-producer"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
  image_scanning_configuration { scan_on_push = true }
}

resource "aws_ecs_cluster" "main" {
  name = var.name
}

resource "aws_cloudwatch_log_group" "producer" {
  name              = "/ecs/${var.name}-producer"
  retention_in_days = 14
}

resource "aws_ecs_task_definition" "producer" {
  family                   = "${var.name}-producer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.producer_exec.arn
  task_role_arn            = aws_iam_role.producer_task.arn
  runtime_platform {
    cpu_architecture        = "ARM64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([{
    name      = "producer"
    image     = var.producer_image != "" ? var.producer_image : "${aws_ecr_repository.producer.repository_url}:latest"
    essential = true
    environment = [
      { name = "STREAM_NAME", value = aws_kinesis_stream.trades.name },
      { name = "AWS_REGION", value = var.region },
      { name = "SOURCES", value = var.sources },
      { name = "API_SECRET_ID", value = aws_secretsmanager_secret.api_keys.name },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.producer.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "producer"
      }
    }
  }])
}

# Default VPC public subnets keep the network simple and free (no NAT gateway).
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "producer" {
  name   = "${var.name}-producer"
  vpc_id = data.aws_vpc.default.id
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_ecs_service" "producer" {
  name            = "${var.name}-producer"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.producer.arn
  desired_count   = var.producer_desired_count
  launch_type     = "FARGATE"
  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.producer.id]
    assign_public_ip = true
  }
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "producer_exec" {
  name               = "${var.name}-producer-exec"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "producer_exec" {
  role       = aws_iam_role.producer_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "producer_task" {
  name               = "${var.name}-producer-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "producer_task" {
  statement {
    actions   = ["kinesis:PutRecords", "kinesis:PutRecord"]
    resources = [aws_kinesis_stream.trades.arn]
  }
  statement {
    actions   = ["kms:GenerateDataKey"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["kinesis.${var.region}.amazonaws.com"]
    }
  }
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.api_keys.arn]
  }
  statement {
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["StockStream"]
    }
  }
}

resource "aws_iam_role_policy" "producer_task" {
  role   = aws_iam_role.producer_task.id
  policy = data.aws_iam_policy_document.producer_task.json
}
