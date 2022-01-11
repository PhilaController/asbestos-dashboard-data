import os
from io import StringIO

import boto3
from dotenv import find_dotenv, load_dotenv

from .data import load_schools_database


def upload_to_s3(data, filename):
    """Upload data to a public AWS s3 bucket."""

    # Load the credentials
    load_dotenv(find_dotenv())

    # Initialize the s3 resource
    s3_resource = boto3.resource("s3")

    # Write to a buffer
    buffer = StringIO()
    buffer.write(data)

    # Upload to s3
    BUCKET = "asbestos-dashboard"
    s3_resource.Object(BUCKET, filename).put(Body=buffer.getvalue(), ACL="public-read")
