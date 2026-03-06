#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.dynamodb_stack import DynamoDBStack
from stacks.cognito_stack import CognitoStack
from stacks.lambda_stack_separated import LambdaStackSeparated
from stacks.step_function_stack import StepFunctionStack
from stacks.frontend_stack import FrontendStack

app = cdk.App()

env = cdk.Environment(account="593470989724", region="ap-southeast-1")

# ── Stacks ────────────────────────────────────────────────
dynamodb_stack = DynamoDBStack(app, "DynamoDBStack", env=env)

cognito_stack = CognitoStack(app, "CognitoStack", env=env)

step_function_stack = StepFunctionStack(app, "StepFunctionStack", env=env)

lambda_stack = LambdaStackSeparated(
    app,
    "LambdaStackSeparated",
    cognito_stack=cognito_stack,
    env=env,
)
lambda_stack.add_dependency(step_function_stack)
lambda_stack.add_dependency(cognito_stack)

# ── Frontend Stack ─────────────────────────────────────────
# Passes the real API Gateway URL so config.js gets injected correctly
frontend_stack = FrontendStack(
    app,
    "FrontendStack",
    api_url=lambda_stack.api.url,   # e.g. https://xxxx.execute-api.ap-southeast-1.amazonaws.com/prod/
    env=env,
)
frontend_stack.add_dependency(lambda_stack)

app.synth()