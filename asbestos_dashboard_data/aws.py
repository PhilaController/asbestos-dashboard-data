import os
from io import StringIO

import boto3
from dotenv import find_dotenv, load_dotenv

from .data import load_schools_database


def upload_to_s3(data, filename):
    """Upload data to a public AWS s3 bucket."""

    # Load the credentials and check
    load_dotenv(find_dotenv())
    ACCESS_KEY_ID = os.getenv("AWS_KEY")
    SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_KEY")

    if ACCESS_KEY_ID is None:
        raise ValueError("Define 'AWS_KEY' environment variable")
    if SECRET_ACCESS_KEY is None:
        raise ValueError("Define 'AWS_SECRET_KEY' enviroment variable")

    # Initialize the s3 resource
    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY,
    )

    # Write to a buffer
    buffer = StringIO()
    buffer.write(data)

    # Upload to s3
    BUCKET = "asbestos-dashboard"
    s3_resource.Object(BUCKET, filename).put(Body=buffer.getvalue(), ACL="public-read")
