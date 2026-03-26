resource "aws_vpc" "meu_custo_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "meu-custo-vpc"
  }
}

resource "aws_subnet" "meu_custo_subnet" {
  vpc_id                  = aws_vpc.meu_custo_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "us-east-1b"

  tags = {
    Name = "meu-custo-subnet"
  }
}

resource "aws_internet_gateway" "meu_custo_igw" {
  vpc_id = aws_vpc.meu_custo_vpc.id

  tags = {
    Name = "meu-custo-igw"
  }
}

resource "aws_route_table" "meu_custo_route_table" {
  vpc_id = aws_vpc.meu_custo_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.meu_custo_igw.id
  }

  tags = {
    Name = "meu-custo-ec2-rt"
  }
}

resource "aws_route_table_association" "meu_custo_route_table_assoc" {
  subnet_id      = aws_subnet.meu_custo_subnet.id
  route_table_id = aws_route_table.meu_custo_route_table.id
}