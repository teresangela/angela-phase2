from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb
)
from constructs import Construct


class DynamoDBStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        user_table = dynamodb.Table(
            self,
            "UserTable",
            table_name="User",
            partition_key=dynamodb.Attribute(
                name="userId",
                type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE
        )

        # GSI for User: query by email
        user_table.add_global_secondary_index(
            index_name="email-index",
            partition_key=dynamodb.Attribute(
                name="email",
                type=dynamodb.AttributeType.STRING
            ),
        )

        product_table = dynamodb.Table(
            self,
            "ProductTable",
            table_name="Product",
            partition_key=dynamodb.Attribute(
                name="productId",
                type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE
        )

        # GSI for Product: query by category
        product_table.add_global_secondary_index(
            index_name="category-index",
            partition_key=dynamodb.Attribute(
                name="category",
                type=dynamodb.AttributeType.STRING
            ),
        )