provider "aws" {
  region = var.aws_region
}

# S3 Bucket for data storage
resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = "${var.project_name}-${var.environment}-datalake"
  force_destroy = true
}

# EC2 Instance for running components
resource "aws_instance" "data_platform" {
  ami           = var.ec2_ami
  instance_type = "t3.medium"
  key_name      = var.key_pair_name

  tags = {
    Name = "${var.project_name}-${var.environment}-server"
  }

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  vpc_security_group_ids = [aws_security_group.data_platform_sg.id]
}

# Security Group
resource "aws_security_group" "data_platform_sg" {
  name        = "${var.project_name}-${var.environment}-sg"
  description = "Security group for data platform"

  # SSH access
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Kafka
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Metabase
  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Kestra
  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}