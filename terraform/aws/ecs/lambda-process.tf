
resource "aws_lambda_function" "ecs_trigger_process" {
  function_name = "ecs-trigger-process"
  role          = aws_iam_role.lambda_role.arn
  runtime       = "python3.11"
  handler       = "index.lambda_handler"

  filename         = "lambda_process.zip"
  source_code_hash = filebase64sha256("lambda_process.zip")

  environment {
    variables = {
      CLUSTER_ARN          = aws_ecs_cluster.app_cluster.arn
      TASK_DEFINITION_ARN  = aws_ecs_task_definition.app_task_process.arn
      SUBNETS              = var.subnet_id
      SECURITY_GROUPS      = var.vpc_security_group_ids
      NOME_CONTAINER        = var.nome_container_process

    }
  }
}

##########################
# 6. API Gateway HTTP
##########################
resource "aws_apigatewayv2_api" "ecs_api_process" {
  name          = "ecs-api-process"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "lambda_integration_process" {
  api_id                 = aws_apigatewayv2_api.ecs_api_process.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.ecs_trigger_process.invoke_arn
  integration_method     = "POST"
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "ecs_route_process" {
  api_id    = aws_apigatewayv2_api.ecs_api_process.id
  route_key = "POST /run"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration_process.id}"
}

resource "aws_lambda_permission" "apigw_lambda_process" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ecs_trigger_process.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.ecs_api_process.execution_arn}/*/*"
}

resource "aws_apigatewayv2_stage" "ecs_stage_process" {
  api_id      = aws_apigatewayv2_api.ecs_api_process.id
  name        = "$default"
  auto_deploy = true
}

output "api_url_process" {
  value = aws_apigatewayv2_stage.ecs_stage_process.invoke_url
}