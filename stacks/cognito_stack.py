from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    RemovalPolicy,
    aws_cognito as cognito,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
)
from constructs import Construct


class CognitoStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # 1. User Pool
        user_pool = cognito.UserPool(
            self,
            "HyPhase2UserPool",
            user_pool_name="hy-phase2-user-pool",
            self_sign_up_enabled=True,
            sign_in_aliases=cognito.SignInAliases(email=True),
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_uppercase=True,
                require_digits=True,
                require_symbols=False,
            ),
            account_recovery=cognito.AccountRecovery.EMAIL_ONLY,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # 2. User Pool Client
        user_pool_client = cognito.UserPoolClient(
            self,
            "HyPhase2UserPoolClient",
            user_pool=user_pool,
            user_pool_client_name="hy-phase2-client",
            generate_secret=False,
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True,
            ),
            access_token_validity=Duration.hours(1),
            id_token_validity=Duration.hours(1),
            refresh_token_validity=Duration.days(30),
        )

        # 3. Post-Confirmation Lambda
        user_table = dynamodb.Table.from_table_name(
            self, "UserTableCognito", "User"
        )

        post_confirmation_fn = _lambda.Function(
            self,
            "PostConfirmationFn",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/PostConfirmation"),
            environment={"USER_TABLE_NAME": "User"},
        )
        user_table.grant_read_write_data(post_confirmation_fn)

        # 4. Attach trigger
        user_pool.add_trigger(
            cognito.UserPoolOperation.POST_CONFIRMATION,
            post_confirmation_fn,
        )

        # 5. Exports
        self.user_pool = user_pool
        self.user_pool_client = user_pool_client

        CfnOutput(self, "UserPoolId",
                  value=user_pool.user_pool_id,
                  export_name="HyPhase2UserPoolId")
        CfnOutput(self, "UserPoolClientId",
                  value=user_pool_client.user_pool_client_id,
                  export_name="HyPhase2UserPoolClientId")
