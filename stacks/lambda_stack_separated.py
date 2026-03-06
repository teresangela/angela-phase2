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
    aws_s3 as s3,
    aws_scheduler as scheduler,   
    RemovalPolicy,                 
    
)
from aws_cdk.aws_lambda_event_sources import DynamoEventSource
from aws_cdk.aws_lambda import StartingPosition
from constructs import Construct
from aws_cdk import aws_sqs as sqs
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import Fn
from aws_cdk import aws_apigateway as apigw



class LambdaStackSeparated(Stack):
    def __init__(self, scope: Construct, construct_id: str, cognito_stack=None, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------
        # Lambda Layer
        # ------------------------
        powertools_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "PowertoolsLayer",
            "arn:aws:lambda:ap-southeast-1:017000801446:layer:AWSLambdaPowertoolsPythonV3-python312-x86_64:7",
        )

        deps_layer = _lambda.LayerVersion(
            self,
            "DepsLayer",
            code=_lambda.Code.from_asset("layers_deps"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Dependencies layer for requests and requests-aws4auth",
        )

        util_layer = _lambda.LayerVersion(
            self,
            "UtilLayer",
            code=_lambda.Code.from_asset("layers_utils"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Utils layer (opensearch_helper, ddb_stream_helper)",
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

        uploads_bucket = s3.Bucket.from_bucket_name(
            self,
            "UploadsBucketImport",
            "angel-phase2-uploads",
        )

        # ------------------------
        # SQS FIFO - Order Queue
        # ------------------------
        order_dlq = sqs.Queue(
            self,
            "OrderDLQ",
            queue_name="OrderDLQ.fifo",
            fifo=True,
        )

        order_queue = sqs.Queue(
            self,
            "OrderQueue",
            queue_name="OrderQueue.fifo",
            fifo=True,
            content_based_deduplication=False,
            visibility_timeout=Duration.seconds(30),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=order_dlq,
            ),
        )

        # ------------------------
        # Order Table (buat idempotency)
        # ------------------------
        order_table = dynamodb.Table.from_table_name(
            self,
            "OrderTableImport",
            "Order",
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

        get_upload_url = create_lambda(
            "GetUploadUrlSep",
            "lambda/GetUploadUrl",
            {
                "USER_TABLE_NAME": "User",
                "UPLOAD_BUCKET": uploads_bucket.bucket_name,
            },
        )

        get_download_url = create_lambda(
            "GetDownloadUrlSep",
            "lambda/GetDownloadUrl",
            {
                "USER_TABLE_NAME": "User",
                "UPLOAD_BUCKET": uploads_bucket.bucket_name,
            },
        )

        create_order = create_lambda(
            "CreateOrderSep",
            "lambda/CreateOrder",
            {
                "ORDER_QUEUE_URL": order_queue.queue_url,
                "ORDER_TABLE_NAME": "Order",
            },
        )

        process_order = create_lambda(
            "ProcessOrderSep",
            "lambda/ProcessOrder",
            {
                "ORDER_TABLE_NAME": "Order",
            },
        )

        all_functions = [
            create_user,
            get_user,
            update_user,
            delete_user,
            create_product,
            get_product,
            update_product,
            delete_product,
            get_upload_url,
            get_download_url,
            create_order,
            process_order,
        ]

        # Grant CloudWatch PutMetricData permission
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

            user_table.grant_read_write_data(get_upload_url)
            user_table.grant_read_data(get_download_url)

            uploads_bucket.grant_put(get_upload_url)
            uploads_bucket.grant_read(get_download_url)

        # ------------------------
        # SQS + Order Table Permissions
        # ------------------------
        order_queue.grant_send_messages(create_order)
        order_table.grant_read_write_data(create_order)
        order_table.grant_read_write_data(process_order)

        process_order.add_event_source(
            SqsEventSource(
                order_queue,
                batch_size=10,
            )
        )

        # allow GetUser lambda to Query the GSI (email-index)
        get_user.add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:Query"],
                resources=[
                    user_table.table_arn,
                    f"{user_table.table_arn}/index/*",
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
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=apigw.Cors.ALL_METHODS,
                allow_headers=["Authorization", "Content-Type"],
            ),
        )
        self.api = api
        state_machine_arn = Fn.import_value("StripeStateMachineArn")

        # Cognito Authorizer
        authorizer = apigw.CognitoUserPoolsAuthorizer(
            self,
            "HyPhase2Authorizer",
            cognito_user_pools=[cognito_stack.user_pool],
        )
        start_payment_workflow = _lambda.Function(
            self,
            "StartPaymentWorkflowFn",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.handler",  # ← fix this
            code=_lambda.Code.from_asset("lambda/StartPaymentWorkflow"),
            environment={"STATE_MACHINE_ARN": state_machine_arn},
        )

        start_payment_workflow.add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[state_machine_arn],
            )
        )

        payments = api.root.add_resource("payments")
        payments.add_method("POST", apigw.LambdaIntegration(start_payment_workflow),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

        users = api.root.add_resource("users")
        users.add_method("POST", apigw.LambdaIntegration(create_user))  # public - sign up
        users.add_method("GET", apigw.LambdaIntegration(get_user),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

        user_id = users.add_resource("{userId}")
        user_id.add_method("GET", apigw.LambdaIntegration(get_user),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        user_id.add_method("PUT", apigw.LambdaIntegration(update_user),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        user_id.add_method("DELETE", apigw.LambdaIntegration(delete_user),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

        upload_url = user_id.add_resource("upload-url")
        upload_url.add_method("POST", apigw.LambdaIntegration(get_upload_url),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

        download_url = user_id.add_resource("download-url")
        download_url.add_method("GET", apigw.LambdaIntegration(get_download_url),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

        products = api.root.add_resource("products")
        products.add_method("POST", apigw.LambdaIntegration(create_product),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        products.add_method("GET", apigw.LambdaIntegration(get_product))  # public - browse

        product_id = products.add_resource("{productId}")
        product_id.add_method("GET", apigw.LambdaIntegration(get_product))  # public - browse
        product_id.add_method("PUT", apigw.LambdaIntegration(update_product),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )
        product_id.add_method("DELETE", apigw.LambdaIntegration(delete_product),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

        orders = api.root.add_resource("orders")
        orders.add_method("POST", apigw.LambdaIntegration(create_order),
            authorization_type=apigw.AuthorizationType.COGNITO,
            authorizer=authorizer,
        )

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
            tls_security_policy=opensearch.TLSSecurityPolicy.TLS_1_2,
        )

        # ------------------------
        # Stream Lambda: DDB -> ES
        # ------------------------
        stream_ddb_to_es = create_lambda(
            "StreamDDBToES",
            "lambda/StreamDDBToES",
            {"ES_DOMAIN_ENDPOINT": domain.domain_endpoint},
            extra_layers=[deps_layer, util_layer],
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

        # ════════════════════════════════════════════════════════════════════════
        # DAY 4 — Batch Report Job
        # EventBridge Scheduler → ReportGeneratorFn → CSV → S3 + DynamoDB job log
        # ════════════════════════════════════════════════════════════════════════

        # ------------------------
        # S3 Bucket: reports-bucket
        # ------------------------
        reports_bucket = s3.Bucket(
            self,
            "ReportsBucket",
            bucket_name="angel-phase2-reports",
            removal_policy=RemovalPolicy.RETAIN,  # keep reports on stack destroy
        )

        # ------------------------
        # DynamoDB Table: ReportJob (job records)
        # ------------------------
        report_job_table = dynamodb.Table(
            self,
            "ReportJobTable",
            table_name="ReportJob",
            partition_key=dynamodb.Attribute(
                name="jobId",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ------------------------
        # Lambda: ReportGeneratorFn
        # ------------------------
        report_generator = create_lambda(
            "ReportGeneratorFn",
            "lambda/ReportGenerator",
            {
                "REPORTS_BUCKET":   reports_bucket.bucket_name,
                "ORDER_TABLE_NAME": "Order",
                "JOBS_TABLE_NAME":  report_job_table.table_name,
            },
        )

        # IAM permissions for ReportGeneratorFn
        reports_bucket.grant_put(report_generator)          # s3:PutObject on reports/*
        order_table.grant_read_data(report_generator)       # read orders
        report_job_table.grant_read_write_data(report_generator)  # write job records

        # CloudWatch alarm for ReportGeneratorFn
        report_alarm = report_generator.metric_errors().create_alarm(
            self,
            "ReportGeneratorFnErrorAlarm",
            alarm_name="ReportGeneratorFn-errors",
            threshold=1,
            evaluation_periods=1,
        )
        report_alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))

        # ------------------------
        # IAM Role for EventBridge Scheduler
        # (Scheduler needs its own role to invoke Lambda)
        # ------------------------
        scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
            description="Allows EventBridge Scheduler to invoke ReportGeneratorFn",
        )
        report_generator.grant_invoke(scheduler_role)

        # ------------------------
        # EventBridge Scheduler: daily at 23:00 UTC
        # ------------------------
        scheduler.CfnSchedule(
            self,
            "DailyReportSchedule",
            name="DailyOrderReportSchedule",
            description="Triggers ReportGeneratorFn every day at 23:00 UTC",
            schedule_expression="cron(0 23 * * ? *)",       # daily 23:00 UTC
            flexible_time_window=scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                mode="OFF",                                  # exact time, no flexibility
            ),
            target=scheduler.CfnSchedule.TargetProperty(
                arn=report_generator.function_arn,
                role_arn=scheduler_role.role_arn,
                input="{}"                                   # empty payload
            ),
        )

        # ------------------------
        # Lambda: SendReportEmailFn
        # ------------------------
        send_report_email = create_lambda(
            "SendReportEmailFn",
            "lambda/SendReportEmail",
            {
                "SENDER_EMAIL":    "teresangelaa.rosa@gmail.com",   # ← replace with your verified SES email
                "RECIPIENT_EMAIL": "teresangelaa.rosa@gmail.com",   # ← replace with recipient email
                "REPORTS_BUCKET":  reports_bucket.bucket_name,
            },
        )

        # Allow Lambda to send email via SES
        send_report_email.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ses:SendEmail", "ses:SendRawEmail"],
                resources=["*"],
            )
        )

        # S3 event trigger → SendReportEmailFn when CSV lands in reports/*
        reports_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(send_report_email),
            s3.NotificationKeyFilter(prefix="reports/", suffix=".csv"),
        )

        # CloudWatch alarm for SendReportEmailFn
        email_alarm = send_report_email.metric_errors().create_alarm(
            self,
            "SendReportEmailFnErrorAlarm",
            alarm_name="SendReportEmailFn-errors",
            threshold=1,
            evaluation_periods=1,
        )
        email_alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))

        # ------------------------
        # DynamoDB Table: ExportJob
        # ------------------------
        export_job_table = dynamodb.Table(
            self,
            "ExportJobTable",
            table_name="ExportJob",
            partition_key=dynamodb.Attribute(
                name="jobId",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ------------------------
        # SQS FIFO - Export Job Queue
        # ------------------------
        export_job_dlq = sqs.Queue(
            self,
            "ExportJobDLQ",
            queue_name="ExportJobDLQ.fifo",
            fifo=True,
        )

        export_job_queue = sqs.Queue(
            self,
            "ExportJobQueue",
            queue_name="ExportJobQueue.fifo",
            fifo=True,
            content_based_deduplication=False,
            visibility_timeout=Duration.seconds(60),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=export_job_dlq,
            ),
        )

        # ------------------------
        # Lambda: GetOrderList
        # ------------------------
        get_order_list = create_lambda(
            "GetOrderListFn",
            "lambda/GetOrderList",
            {"ORDER_TABLE_NAME": "Order"},
        )
        order_table.grant_read_data(get_order_list)

        # ------------------------
        # Lambda: CreateExportJob
        # ------------------------
        create_export_job = create_lambda(
            "CreateExportJobFn",
            "lambda/CreateExportJob",
            {
                "EXPORT_JOB_TABLE": export_job_table.table_name,
                "EXPORT_QUEUE_URL": export_job_queue.queue_url,
            },
        )
        export_job_table.grant_read_write_data(create_export_job)
        export_job_queue.grant_send_messages(create_export_job)

        # ------------------------
        # Lambda: GetExportJob
        # ------------------------
        get_export_job = create_lambda(
            "GetExportJobFn",
            "lambda/GetExportJob",
            {
                "EXPORT_JOB_TABLE": export_job_table.table_name,
                "REPORTS_BUCKET":   reports_bucket.bucket_name,
            },
        )
        export_job_table.grant_read_data(get_export_job)
        reports_bucket.grant_read(get_export_job)

        # ------------------------
        # Lambda: ProcessExportJob (SQS worker)
        # ------------------------
        process_export_job = create_lambda(
            "ProcessExportJobFn",
            "lambda/ProcessExportJob",
            {
                "EXPORT_JOB_TABLE": export_job_table.table_name,
                "ORDER_TABLE_NAME": "Order",
                "REPORTS_BUCKET":   reports_bucket.bucket_name,
            },
        )
        export_job_table.grant_read_write_data(process_export_job)
        order_table.grant_read_data(process_export_job)
        reports_bucket.grant_put(process_export_job)

        process_export_job.add_event_source(
            SqsEventSource(
                export_job_queue,
                batch_size=1,
            )
        )

        # ------------------------
        # CloudWatch Alarms for new Lambdas
        # ------------------------
        for fn in [get_order_list, create_export_job, get_export_job, process_export_job]:
            alarm = fn.metric_errors().create_alarm(
                self,
                f"{fn.node.id}ErrorAlarm",
                alarm_name=f"{fn.node.id}-errors",
                threshold=1,
                evaluation_periods=1,
            )
            alarm.add_alarm_action(cw_actions.SnsAction(alarm_topic))

        # ------------------------
        # API Gateway routes
        # ------------------------
        # GET /orders
        orders.add_method("GET", apigw.LambdaIntegration(get_order_list))

        # /exports
        exports = api.root.add_resource("exports")
        exports_orders = exports.add_resource("orders")
        exports_orders.add_method("POST", apigw.LambdaIntegration(create_export_job))

        # /exports/orders/{jobId}
        export_job_id = exports_orders.add_resource("{jobId}")
        export_job_id.add_method("GET", apigw.LambdaIntegration(get_export_job))


