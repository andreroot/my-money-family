# Configure IAM Glue
# Configure IAM Glue
resource "aws_iam_role" "aws_iam_mycust_role" {
  name = "AWSMyCust-pipeline-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "mycust_service_attachment" {
  #name = "AWSGlueServiceRole-stream"
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
  role = aws_iam_role.aws_iam_mycust_role.id
}