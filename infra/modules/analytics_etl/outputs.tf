/*===============================
#              Athena
===============================*/

output "s3" {
  value = aws_s3_bucket.analytics_bucket
}

output "athena_db" {
  value = aws_athena_database.athena_db
}
