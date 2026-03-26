# Minimal skeleton for a rustream deployment on AWS.
# Creates S3 bucket + IAM role/policy for ECS task or instance profile.

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "rustream" {
  bucket = var.bucket_name
  force_destroy = true
}

data "aws_iam_policy_document" "rustream" {
  statement {
    actions   = ["s3:*"]
    resources = [aws_s3_bucket_rustream.arn, "${aws_s3_bucket_rustream.arn}/*"]
  }
}

resource "aws_iam_role" "rustream" {
  name               = "${var.name_prefix}-role"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "rustream" {
  name   = "${var.name_prefix}-policy"
  policy = data.aws_iam_policy_document.rustream.json
}

resource "aws_iam_role_policy_attachment" "rustream" {
  role       = aws_iam_role.rustream.name
  policy_arn = aws_iam_policy.rustream.arn
}

output "bucket" {
  value = aws_s3_bucket.rustream.bucket
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "bucket_name" {
  type    = string
  default = "rustream-demo-bucket"
}

variable "name_prefix" {
  type    = string
  default = "rustream-demo"
}
