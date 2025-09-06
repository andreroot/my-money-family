resource "tls_private_key" "ec2_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "aws_key_pair" "my_ec2_key" {
  key_name   = "my-ec2-key-terraform"
  public_key = tls_private_key.ec2_key.public_key_openssh
}

output "private_key" {
  value     = tls_private_key.ec2_key.private_key_pem
  sensitive = true
}