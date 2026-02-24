from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigw,
)
from constructs import Construct

class LambdaStackSeparated(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Import existing tables
        user_table = dynamodb.Table.from_table_name(self, "UserTableImport", "User")
        product_table = dynamodb.Table.from_table_name(self, "ProductTableImport", "Product")

        # User Functions
        create_user = _lambda.Function(self, "CreateUserSep",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/CreateUser"),
            environment={"USER_TABLE_NAME": "User"})

        get_user = _lambda.Function(self, "GetUserSep",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/GetUser"),
            environment={"USER_TABLE_NAME": "User"})

        update_user = _lambda.Function(self, "UpdateUserSep",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/UpdateUser"),
            environment={"USER_TABLE_NAME": "User"})

        delete_user = _lambda.Function(self, "DeleteUserSep",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/DeleteUser"),
            environment={"USER_TABLE_NAME": "User"})

        # Product Functions
        create_product = _lambda.Function(self, "CreateProductSep",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/CreateProduct"),
            environment={"PRODUCT_TABLE_NAME": "Product"})

        get_product = _lambda.Function(self, "GetProductSep",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/GetProduct"),
            environment={"PRODUCT_TABLE_NAME": "Product"})

        update_product = _lambda.Function(self, "UpdateProductSep",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/UpdateProduct"),
            environment={"PRODUCT_TABLE_NAME": "Product"})

        delete_product = _lambda.Function(self, "DeleteProductSep",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/DeleteProduct"),
            environment={"PRODUCT_TABLE_NAME": "Product"})

        # Grant DynamoDB permissions
        for fn in [create_user, get_user, update_user, delete_user]:
            user_table.grant_read_write_data(fn)

        for fn in [create_product, get_product, update_product, delete_product]:
            product_table.grant_read_write_data(fn)

        # API Gateway
        api = apigw.RestApi(self, "HyPhase2ApiSeparated",
            rest_api_name="HyPhase2ApiSeparated",
            deploy_options=apigw.StageOptions(stage_name="prod"))

        # /users
        users = api.root.add_resource("users")
        users.add_method("POST", apigw.LambdaIntegration(create_user))
        users.add_method("GET", apigw.LambdaIntegration(get_user))

        user_id = users.add_resource("{userId}")
        user_id.add_method("GET", apigw.LambdaIntegration(get_user))
        user_id.add_method("PUT", apigw.LambdaIntegration(update_user))
        user_id.add_method("DELETE", apigw.LambdaIntegration(delete_user))

        # /products
        products = api.root.add_resource("products")
        products.add_method("POST", apigw.LambdaIntegration(create_product))
        products.add_method("GET", apigw.LambdaIntegration(get_product))

        product_id = products.add_resource("{productId}")
        product_id.add_method("GET", apigw.LambdaIntegration(get_product))
        product_id.add_method("PUT", apigw.LambdaIntegration(update_product))
        product_id.add_method("DELETE", apigw.LambdaIntegration(delete_product))