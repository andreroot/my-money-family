
# Configure the AWS S3
resource "aws_s3_bucket" "s3v2" {
  bucket        = "medalion-cust"
  force_destroy = true
  tags = {
      Environment     = "staging"
      Name = "geralog-kinesis-s3"
  }
}


resource "aws_s3_bucket_ownership_controls" "s3v2" {
  bucket = "medalion-cust"
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
  depends_on = [aws_s3_bucket.s3v2]
}

resource "aws_s3_bucket_acl" "s3v2" {
  depends_on = [aws_s3_bucket_ownership_controls.s3v2]

  bucket = "medalion-cust"
  acl    = "private"

}



resource "aws_s3_object" "s3v2" {
  key        = "raw/"
  bucket     = aws_s3_bucket.s3v2.id
  #source     = "pathlocal/policy.json"
  #kms_key_id = aws_kms_key.examplekms.arn
  force_destroy = true
}

resource "aws_s3_object" "s3v2_processed" {
  key        = "processed/"
  bucket     = aws_s3_bucket.s3v2.id
  #source     = "pathlocal/policy.json"
  #kms_key_id = aws_kms_key.examplekms.arn
  force_destroy = true
}

