import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import os

sns_client = boto3.client("sns")
region = os.environ["AWS_REGION"]  # Lambda's region
s3_client = boto3.client("s3", region_name=region)

SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:067249855895:notification"
LOG_BUCKET = f"awsnotification-logs-{region}-067249855895"  # Unique bucket name per region

def ensure_bucket_exists(bucket_name):
    """Check if bucket exists, create if not."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
    except ClientError as e:
        if e.response['Error']['Code'] in ("404", "NoSuchBucket"):
            print(f"Bucket '{bucket_name}' not found. Creating...")
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
            print(f"Bucket '{bucket_name}' created in {region}.")
        else:
            raise

def lambda_handler(event, context):
    try:
        print("Received event:", json.dumps(event))

        # Ensure log bucket exists
        ensure_bucket_exists(LOG_BUCKET)

        # Extract S3 info from event
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]
        file_size = record["s3"]["object"]["size"]
        event_time = record["eventTime"]

        message = (
            f"📢 New file uploaded to S3!\n\n"
            f"🪣 Bucket: {bucket_name}\n"
            f"📄 File: {object_key}\n"
            f"📏 Size: {file_size} bytes\n"
            f"🕒 Time: {event_time}"
        )

        subject = f"New S3 Upload: {object_key[:50]}"

        # Send SNS notification
        sns_response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=subject
        )

        print("SNS Response:", sns_response)

        # Save log to S3
        log_key = f"logs/{datetime.utcnow().isoformat()}_{object_key}.json"
        s3_client.put_object(
            Bucket=LOG_BUCKET,
            Key=log_key,
            Body=json.dumps({
                "bucket": bucket_name,
                "file": object_key,
                "size": file_size,
                "time": event_time,
                "sns_message_id": sns_response["MessageId"]
            }),
            ContentType="application/json"
        )

        print(f"Log saved to s3://{LOG_BUCKET}/{log_key}")

        return {"statusCode": 200, "body": json.dumps({"message": "Notification sent and logged"})}

    except Exception as e:
        print(f"Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
