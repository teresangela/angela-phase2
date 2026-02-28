import aws_cdk as cdk

from stacks.dynamodb_stack import DynamoDBStack
from stacks.lambda_stack import LambdaStack
from stacks.lambda_stack_separated import LambdaStackSeparated
from stacks.step_function_stack import StepFunctionStack
from stacks.cognito_stack import CognitoStack  

app = cdk.App()

DynamoDBStack(app, "DynamoDBStack")
LambdaStack(app, "LambdaStack")

sf_stack = StepFunctionStack(app, "StepFunctionStack")
cognito_stack = CognitoStack(app, "CognitoStack")  
lambda_sep_stack = LambdaStackSeparated(app, "LambdaStackSeparated", cognito_stack=cognito_stack)
lambda_sep_stack.add_dependency(sf_stack)
lambda_sep_stack.add_dependency(cognito_stack) 

app.synth()