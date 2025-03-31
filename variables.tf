variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "movielens"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "ec2_ami" {
  description = "AMI ID for EC2 instance"
  type        = string
  default     = "ami-0261755bbcb8c4a84"  # Amazon Linux 2023 AMI
}

variable "key_pair_name" {
  description = "Key pair name for EC2 SSH access"
  type        = string
}