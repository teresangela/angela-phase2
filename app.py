#!/usr/bin/env python3
import aws_cdk as cdk

from hy_phase2.dynamodb_stack import DynamoDBStack
from hy_phase2.lambda_stack import LambdaStack
from hy_phase2.lambda_stack_separated import LambdaStackSeparated

app = cdk.App()

DynamoDBStack(app, "DynamoDBStack"),
LambdaStack(app, "LambdaStack"),
LambdaStackSeparated(app, "LambdaStackSeparated")

app.synth()