terraform {
  required_version = ">= 1.6"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.60" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
  # Remote state is optional for a one-person project. To use it, create the bucket once and uncomment:
  # backend "s3" { bucket = "<your-tfstate-bucket>", key = "stockstream/terraform.tfstate", region = "us-east-1" }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "stockstream", managed_by = "terraform" }
  }
}

data "aws_caller_identity" "me" {}
