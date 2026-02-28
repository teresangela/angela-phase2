from aws_cdk import Stack, Duration
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from aws_cdk import aws_lambda as _lambda
from constructs import Construct


class StepFunctionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, stripe_payment_fn: _lambda.IFunction, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        stripe_task = tasks.LambdaInvoke(
            self,
            "CallStripePayment",
            lambda_function=stripe_payment_fn,
            output_path="$.Payload",
        )

        # ✅ Retry policy: 3 attempts, exponential backoff
        stripe_task.add_retry(
            errors=["RateLimited", "States.ALL"],
            interval=Duration.seconds(2),
            max_attempts=3,
            backoff_rate=2.0,
        )

        self.state_machine = sfn.StateMachine(
            self,
            "StripeRetryStateMachine",
            definition=stripe_task,
        )