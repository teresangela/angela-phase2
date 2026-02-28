from aws_cdk import Stack, Duration, CfnOutput
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

class StepFunctionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # import Order table (sama kayak kamu)
        order_table = dynamodb.Table.from_table_name(self, "OrderTableImportSF", "Order")

        # import secret
        stripe_secret = secretsmanager.Secret.from_secret_name_v2(
            self, "StripeSecretSF", "stripe/secret-key"
        )

        # StripePaymentFn PINDAH ke sini
        stripe_payment_fn = _lambda.Function(
            self,
            "StripePaymentFn",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/StripePayment"),
            environment={
                "ORDER_TABLE_NAME": "Order",
                "STRIPE_SECRET_ARN": stripe_secret.secret_arn,
            },
        )
        order_table.grant_read_write_data(stripe_payment_fn)
        stripe_secret.grant_read(stripe_payment_fn)

        # Step Function task invoke Stripe lambda
        stripe_task = tasks.LambdaInvoke(
            self,
            "CallStripePayment",
            lambda_function=stripe_payment_fn,
            output_path="$.Payload",
        )

        # Retry policy (pilih salah satu cara)
        # Cara 1 (paling simpel, tahan banting):
        stripe_task.add_retry(
            errors=["States.ALL"],   # HARUS sendirian (ga boleh dicampur)
            interval=Duration.seconds(2),
            max_attempts=3,
            backoff_rate=2.0,
        )

        # State machine
        state_machine = sfn.StateMachine(
            self,
            "StripeRetryStateMachine",
            definition=stripe_task,
            timeout=Duration.minutes(5),
        )

        # Export ARN biar bisa dipakai stack lain
        CfnOutput(
            self,
            "StripeStateMachineArn",
            value=state_machine.state_machine_arn,
            export_name="StripeStateMachineArn",
        )