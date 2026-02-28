from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigw,
)
from constructs import Construct

class LambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Import existing tables
        user_table = dynamodb.Table.from_table_name(self, "UserTableImport", "User")
        product_table = dynamodb.Table.from_table_name(self, "ProductTableImport", "Product")

        # --- Lambdas ---
        user_fn = _lambda.Function(
            self,
            "UserCrudFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="user_handler.handler",
            code=_lambda.Code.from_asset("lambda"),
            environment={"USER_TABLE_NAME": "User"},
        )

        product_fn = _lambda.Function(
            self,
            "ProductCrudFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="product_handler.handler",
            code=_lambda.Code.from_asset("lambda"),
            environment={"PRODUCT_TABLE_NAME": "Product"},
        )

        # Grant DynamoDB permissions
        user_table.grant_read_write_data(user_fn)
        product_table.grant_read_write_data(product_fn)

        # --- Single API Gateway ---
        api = apigw.RestApi(
            self,
            "HyPhase2Api",
            rest_api_name="HyPhase2Api",
            deploy_options=apigw.StageOptions(stage_name="prod"),
        )

        # /users
        users = api.root.add_resource("users")
        users.add_method("GET", apigw.LambdaIntegration(user_fn))
        users.add_method("POST", apigw.LambdaIntegration(user_fn))

        # /users/{userId}
        user_item = users.add_resource("{userId}")
        user_item.add_method("GET", apigw.LambdaIntegration(user_fn))
        user_item.add_method("PUT", apigw.LambdaIntegration(user_fn))
        user_item.add_method("DELETE", apigw.LambdaIntegration(user_fn))

        # /products
        products = api.root.add_resource("products")
        products.add_method("GET", apigw.LambdaIntegration(product_fn))
        products.add_method("POST", apigw.LambdaIntegration(product_fn))

        # /products/{productId}
        product_item = products.add_resource("{productId}")
        product_item.add_method("GET", apigw.LambdaIntegration(product_fn))
        product_item.add_method("PUT", apigw.LambdaIntegration(product_fn))
        product_item.add_method("DELETE", apigw.LambdaIntegration(product_fn))