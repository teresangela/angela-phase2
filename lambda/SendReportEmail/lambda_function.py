import os
import json
import boto3
from datetime import datetime, timezone
from botocore.config import Config

_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-1"
_config = Config(connect_timeout=5, read_timeout=10)

ses = boto3.client("ses", region_name=_region, config=_config)
s3  = boto3.client("s3",  region_name=_region, config=_config)

SENDER_EMAIL     = os.environ["SENDER_EMAIL"]
RECIPIENT_EMAIL  = os.environ["RECIPIENT_EMAIL"]
REPORTS_BUCKET   = os.environ["REPORTS_BUCKET"]
PRESIGNED_EXPIRY = int(os.environ.get("PRESIGNED_EXPIRY", 3600))


def lambda_handler(event, context):
    for record in event["Records"]:
        bucket   = record["s3"]["bucket"]["name"]
        key      = record["s3"]["object"]["key"]
        size     = record["s3"]["object"]["size"]
        now      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        filename = key.split("/")[-1]

        print(f"[OK] New report detected: s3://{bucket}/{key}")

        # Generate presigned URL (works with private buckets)
        presigned_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=PRESIGNED_EXPIRY,
        )

        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [RECIPIENT_EMAIL]},
            Message={
                "Subject": {
                    "Data": f"📊 Daily Order Report Ready — {filename}",
                },
                "Body": {
                    "Html": {
                        "Data": f"""
                        <html>
                        <body style="font-family: Arial, sans-serif; padding: 20px;">
                            <h2>📊 Daily Order Report Ready</h2>
                            <p>Your scheduled order report has been generated and uploaded to S3.</p>
                            <table style="border-collapse: collapse; width: 100%;">
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>File</strong></td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">{filename}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Bucket</strong></td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">{bucket}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>S3 Key</strong></td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">{key}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Size</strong></td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">{size} bytes</td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Generated At</strong></td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">{now}</td>
                                </tr>
                                <tr>
                                    <td style="padding: 8px; border: 1px solid #ddd;"><strong>Link expires</strong></td>
                                    <td style="padding: 8px; border: 1px solid #ddd;">in {PRESIGNED_EXPIRY // 3600} hour(s)</td>
                                </tr>
                            </table>
                            <br>
                            <p>
                                <a href="{presigned_url}" style="background-color: #FF9900; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px;">
                                    Download Report
                                </a>
                            </p>
                            <p style="color: #888; font-size: 12px;">This is an automated message from your HY-Phase2 reporting system.</p>
                        </body>
                        </html>
                        """,
                    },
                    "Text": {
                        "Data": f"Daily Order Report Ready\n\nFile: {filename}\nBucket: {bucket}\nKey: {key}\nSize: {size} bytes\nGenerated At: {now}\n\nDownload (expires in {PRESIGNED_EXPIRY // 3600}h): {presigned_url}",
                    },
                },
            },
        )

        print(f"[OK] Email sent to {RECIPIENT_EMAIL} for report {filename}")