
# data "aws_iam_policy" "this" {
#   name = "GetSecrets"
# }

resource "aws_iam_role" "my_ec2_execution_role" {
  name = "my-ec2-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

data "aws_iam_policy" "management_group_policy" {
  for_each = toset(["AmazonEC2ContainerRegistryReadOnly", "AmazonECSTaskExecutionRolePolicy"])
  name     = each.value
}

# politica para log stream no cloudwatch
resource "aws_iam_role_policy_attachment" "ec2_execution_role_policy" {
  role       = aws_iam_role.my_ec2_execution_role.name
  for_each = data.aws_iam_policy.management_group_policy
  policy_arn = each.value.arn 
}
#["arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly", "arn:aws:iam::aws:policy/AmazonECSTaskExecutionRolePolicy"]

# resource "aws_iam_role_policy_attachment" "secrets_manager" {
#   role       = aws_iam_role.ecs_webhook_execution_role.name
#   policy_arn = data.aws_iam_policy.this.arn
# }