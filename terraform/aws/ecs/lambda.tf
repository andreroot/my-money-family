
resource "aws_lambda_function" "ecs_trigger" {
  function_name = "ecs-trigger"
  role          = aws_iam_role.lambda_role.arn
  runtime       = "python3.11"
  handler       = "index.lambda_handler"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  environment {
    variables = {
      CLUSTER_ARN          = aws_ecs_cluster.app_cluster.arn
      TASK_DEFINITION_ARN  = aws_ecs_task_definition.app_task.arn
      SUBNETS              = var.subnet_id
      SECURITY_GROUPS      = var.vpc_security_group_ids
      NOME_CONTAINER        = var.nome_container
    }
  }
}

##########################
# 6. API Gateway HTTP
##########################
resource "aws_apigatewayv2_api" "ecs_api" {
  name          = "ecs-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "lambda_integration" {
  api_id                 = aws_apigatewayv2_api.ecs_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.ecs_trigger.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "ecs_route" {
  api_id    = aws_apigatewayv2_api.ecs_api.id
  route_key = "POST /run"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

resource "aws_lambda_permission" "apigw_lambda" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ecs_trigger.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.ecs_api.execution_arn}/*/*"
}

resource "aws_apigatewayv2_stage" "ecs_stage" {
  api_id      = aws_apigatewayv2_api.ecs_api.id
  name        = "$default"
  auto_deploy = true
}

output "api_url" {
  value = aws_apigatewayv2_stage.ecs_stage.invoke_url
}