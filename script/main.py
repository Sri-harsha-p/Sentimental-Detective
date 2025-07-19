from dotenv import load_dotenv
import boto3
import os

load_dotenv()

access_key=os.getenv("ACCESS_KEY")
secret_access_key=os.getenv("SECRET_ACCESS_KEY")
aws_region=os.getenv("AWS_DEFAULT_REGION")
local_path=os.getenv("REVIEW_FILE_PATH")
bucket_name=os.getenv("BUCKET_NAME")
s3 = boto3.client('s3',
                aws_access_key_id= access_key,
                aws_secret_access_key=secret_access_key,
                region_name=aws_region
    )

if local_path is None:
    raise ValueError("DATA_PATH environment variable is not set.")
key_name = os.path.basename(local_path)
s3.upload_file(
    Filename=local_path,
    Bucket=bucket_name,
    Key=key_name
)

