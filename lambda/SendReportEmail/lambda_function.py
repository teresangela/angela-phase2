import os
import json
import boto3
from datetime import datetime, timezone

ses = boto3.client("ses", region_name="ap-southeast-1")

SENDER_EMAIL    = os.environ["SENDER_EMAIL"]
RECIPIENT_EMAIL = os.environ["RECIPIENT_EMAIL"]
REPORTS_BUCKET  = os.environ["REPORTS_BUCKET"]


def lambda_handler(event, context):
    for record in event["Records"]:
        bucket   = record["s3"]["bucket"]["name"]
        key      = record["s3"]["object"]["key"]
        size     = record["s3"]["object"]["size"]
        now      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        s3_url   = f"https://{bucket}.s3.ap-southeast-1.amazonaws.com/{key}"
        filename = key.split("/")[-1]

        print(f"[OK] New report detected: s3://{bucket}/{key}")

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
                            </table>
                            <br>
                            <p>
                                <a href="{s3_url}" style="background-color: #FF9900; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px;">
                                    View Report in S3
                                </a>
                            </p>
                            <p style="color: #888; font-size: 12px;">This is an automated message from your HY-Phase2 reporting system.</p>
                        </body>
                        </html>
                        """,
                    },
                    "Text": {
                        "Data": f"Daily Order Report Ready\n\nFile: {filename}\nBucket: {bucket}\nKey: {key}\nSize: {size} bytes\nGenerated At: {now}\n\nS3 URL: {s3_url}",
                    },
                },
            },
        )

        print(f"[OK] Email sent to {RECIPIENT_EMAIL} for report {filename}")