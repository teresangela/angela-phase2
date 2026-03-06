from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
)
from constructs import Construct


class FrontendStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, api_url: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ─────────────────────────────────────────────────────
        # S3 Bucket — private, only CloudFront can read it
        # ─────────────────────────────────────────────────────
        frontend_bucket = s3.Bucket(
            self,
            "FrontendBucket",
            bucket_name="angel-phase2-frontend",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # ─────────────────────────────────────────────────────
        # CloudFront Origin Access Control
        # ─────────────────────────────────────────────────────
        oac = cloudfront.S3OriginAccessControl(
            self,
            "FrontendOAC",
            signing=cloudfront.Signing.SIGV4_NO_OVERRIDE,
        )

        # ─────────────────────────────────────────────────────
        # CloudFront Distribution
        # ─────────────────────────────────────────────────────
        distribution = cloudfront.Distribution(
            self,
            "FrontendDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(
                    frontend_bucket,
                    origin_access_control=oac,
                ),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
            ),
            default_root_object="orders.html",
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/orders.html",
                ),
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/orders.html",
                ),
            ],
        )

        # ─────────────────────────────────────────────────────
        # Deploy frontend files + inject API URL into config.js
        # ─────────────────────────────────────────────────────
        s3deploy.BucketDeployment(
            self,
            "DeployFrontend",
            sources=[
                # Upload static HTML file
                s3deploy.Source.asset("frontend/"),
                # Inject real API URL into config.js at deploy time
                s3deploy.Source.data(
                    "config.js",
                    f'window.APP_CONFIG = {{ apiUrl: "{api_url}" }};'
                ),
            ],
            destination_bucket=frontend_bucket,
            distribution=distribution,
            distribution_paths=["/*"],  # invalidate CloudFront cache on every deploy
        )

     
        # Outputs
        # ─────────────────────────────────────────────────────
        CfnOutput(
            self,
            "FrontendUrl",
            value=f"https://{distribution.distribution_domain_name}",
            description="Open this URL in your browser to access the Orders dashboard",
            export_name="FrontendUrl",
        )