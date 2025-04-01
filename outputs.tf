output "instance_public_ip" {
  description = "Public IP of EC2 instance"
  value       = aws_instance.data_platform.public_ip
}

output "s3_bucket_name" {
  description = "S3 bucket name for data lake"
  value       = aws_s3_bucket.data_lake_bucket.bucket
}