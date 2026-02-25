from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigw,
    aws_sns as sns,
    aws_cloudwatch_actions as cw_actions,
    aws_opensearchservice as opensearch,
    aws_iam as iam,
)
from aws_cdk.aws_lambda_event_sources import DynamoEventSource
from aws_cdk.aws_lambda import StartingPosition
from constructs import Construct


class LambdaStackSeparated(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------
        # Lambda Layer
        # ------------------------
        powertools_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "PowertoolsLayer",
            "arn:aws:lambda:ap-southeast-1:017000801446:layer:AWSLambdaPowertoolsPythonV3-python312-x86_64:7",
        )

        # ------------------------
        # Dependencies Layer (requests, AWS4Auth)
        # ------------------------
        deps_layer = _lambda.LayerVersion(
            self,
            "DepsLayer",
            code=_lambda.Code.from_asset("layers"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Dependencies layer for requests and requests-aws4auth",
        )

        # ------------------------
        # Import DynamoDB Tables
        # ------------------------
        user_table = dynamodb.Table.from_table_attributes(
            self, "UserTableImport",
            table_name="User",
            table_stream_arn="arn:aws:dynamodb:ap-southeast-1:593470989724:table/User/stream/2026-02-23T19:04:01.095"
        )
        product_table = dynamodb.Table.from_table_attributes(
            self, "ProductTableImport",
            table_name="Product",
            table_stream_arn="arn:aws:dynamodb:ap-southeast-1:593470989724:table/Product/stream/2026-02-23T19:04:01.300"
        )
        # ------------------------
        # Helper: Create Lambda
        # ------------------------

        def create_lambda(name, path, env, extra_layers=None):
            layers = [powertools_layer]
            if extra_layers:
                layers.extend(extra_layers)

            return _lambda.Function(
                self,
                name,
                runtime=_lambda.Runtime.PYTHON_3_12,
                handler="lambda_function.lambda_handler",
                code=_lambda.Code.from_asset(path),
                environment=env,
                layers=layers,
    )

        # ------------------------
        # CRUD Lambdas
        # ------------------------
        create_user = create_lambda("CreateUserSep", "lambda/CreateUser", {"USER_TABLE_NAME": "User"})
        get_user = create_lambda("GetUserSep", "lambda/GetUser", {"USER_TABLE_NAME": "User"})
        update_user = create_lambda("UpdateUserSep", "lambda/UpdateUser", {"USER_TABLE_NAME": "User"})
        delete_user = create_lambda("DeleteUserSep", "lambda/DeleteUser", {"USER_TABLE_NAME": "User"})

        create_product = create_lambda("CreateProductSep", "lambda/CreateProduct", {"PRODUCT_TABLE_NAME": "Product"})
        get_product = create_lambda("GetProductSep", "lambda/GetProduct", {"PRODUCT_TABLE_NAME": "Product"})
        update_product = create_lambda("UpdateProductSep", "lambda/UpdateProduct", {"PRODUCT_TABLE_NAME": "Product"})
        delete_product = create_lambda("DeleteProductSep", "lambda/DeleteProduct", {"PRODUCT_TABLE_NAME": "Product"})

        all_functions = [
            create_user,
            get_user,
            update_user,
            delete_user,
            create_product,
            get_product,
            update_product,
            delete_product,
        ]

        # Grant CloudWatch PutMetricData permission  ← ADD THIS BELOW
        for fn in all_functions:
            fn.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"]
                )
            )
            
        # ------------------------
        # DynamoDB Permissions (CRUD)
        # ------------------------
        for fn in [create_user, get_user, update_user, delete_user]:
            user_table.grant_read_write_data(fn)

        # allow GetUser lambda to Query the GSI (email-index)
        get_user.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[
                    user_table.table_arn,
                    f"{user_table.table_arn}/index/*",   # covers email-index
                ],
            )
        )

        for fn in [create_product, get_product, update_product, delete_product]:
            product_table.grant_read_write_data(fn)

        # allow GetProduct lambda to Query the GSI (category-index)
        get_product.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[
                    product_table.table_arn,
                    f"{product_table.table_arn}/index/*",
                ],
            )
        )

        # ------------------------
        # API Gateway
        # ------------------------
        api = apigw.RestApi(
            self,
            "HyPhase2ApiSeparated",
            rest_api_name="HyPhase2ApiSeparated",
            deploy_options=apigw.StageOptions(stage_name="prod"),
        )

        users = api.root.add_resource("users")
        users.add_method("POST", apigw.LambdaIntegration(create_user))
        users.add_method("GET", apigw.LambdaIntegration(get_user))

        user_id = users.add_resource("{userId}")
        user_id.add_method("GET", apigw.LambdaIntegration(get_user))
        user_id.add_method("PUT", apigw.LambdaIntegration(update_user))
        user_id.add_method("DELETE", apigw.LambdaIntegration(delete_user))

        products = api.root.add_resource("products")
        products.add_method("POST", apigw.LambdaIntegration(create_product))
        products.add_method("GET", apigw.LambdaIntegration(get_product))

        product_id = products.add_resource("{productId}")
        product_id.add_method("GET", apigw.LambdaIntegration(get_product))
        product_id.add_method("PUT", apigw.LambdaIntegration(update_product))
        product_id.add_method("DELETE", apigw.LambdaIntegration(delete_product))

        # ------------------------
        # SNS Alarm Topic + Error Alarms
        # ------------------------
        alarm_topic = sns.Topic.from_topic_arn(
            self,
            "AlarmTopic",
            "arn:aws:sns:ap-southeast-1:593470989724:AngelaLambdaAlarm",
        )

        for fn in all_functions:
            alarm = fn.metric_errors().create_alarm(
                self,
                f"{fn.node.id}ErrorAlarm",
                alarm_name=f"{fn.node.id}-errors",
                threshold=1,
                evaluation_periods=1,
            )
            alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))

        # ------------------------
        # Elasticsearch 7.10 Domain (via OpenSearch service)
        # ------------------------
        domain = opensearch.Domain(
            self,
            "HyPhase2Domain",
            domain_name="hy-phase2-domain-v2",
            version=opensearch.EngineVersion.ELASTICSEARCH_7_10,
            capacity=opensearch.CapacityConfig(
                data_nodes=1,
                data_node_instance_type="t3.small.search",
                multi_az_with_standby_enabled=False,
            ),
            ebs=opensearch.EbsOptions(volume_size=10),
            zone_awareness=opensearch.ZoneAwarenessConfig(enabled=False),
            enforce_https=True,
            node_to_node_encryption=True,
            encryption_at_rest=opensearch.EncryptionAtRestOptions(enabled=True),
        )

        # ------------------------
        # Stream Lambda: DDB -> ES
        # ------------------------
        stream_ddb_to_es = create_lambda(
            "StreamDDBToES",
            "lambda/StreamDDBToES",
            {"ES_DOMAIN_ENDPOINT": domain.domain_endpoint},
            extra_layers=[deps_layer],
        )
        # ------------------------
        # Restrictive Domain Access Policy (CRUD + Stream)
        # ------------------------
        domain.add_access_policies(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[fn.grant_principal for fn in all_functions] + [stream_ddb_to_es.grant_principal],
                actions=[
                    "es:ESHttpGet",
                    "es:ESHttpPost",
                    "es:ESHttpPut",
                    "es:ESHttpDelete",
                    "es:ESHttpHead",
                    "es:ESHttpPatch",
                ],
                resources=[domain.domain_arn, f"{domain.domain_arn}/*"],
            )
        )

        # ------------------------
        # Permissions
        # ------------------------
        for fn in all_functions:
            domain.grant_read_write(fn)

        domain.grant_read_write(stream_ddb_to_es)

        # Event Source Mapping (DynamoDB Streams -> Stream Lambda)
        stream_ddb_to_es.add_event_source(
            DynamoEventSource(
                user_table,
                starting_position=StartingPosition.LATEST,
                batch_size=10,
                retry_attempts=3,
            )
        )

        stream_ddb_to_es.add_event_source(
            DynamoEventSource(
                product_table,
                starting_position=StartingPosition.LATEST,
                batch_size=10,
                retry_attempts=3,
            )
        )