import boto3
from typing import Union, List
from os import getenv
from dotenv import load_dotenv

load_dotenv()

aws_key = getenv("aws_key")
aws_secret = getenv("aws_secret")
aws_endpoint = getenv("aws_endpoint")


def path_to_s3(location: str, env: str) -> dict:
    """
    Convert a relative file path into an S3-compatible path with environment prefix.

    This function takes a file path and an environment name (e.g., dev, test, prod),
    constructs an S3 bucket name by prepending the environment, and returns a dictionary
    containing the bucket name, key (path within bucket), and full S3 location.

    Args:
        location (str): A file path string representing the object path inside the bucket.
            Examples: "data/input/file.csv"
        env (str): The environment name used to construct the bucket name (e.g., "dev", "prod").

    Returns:
        dict: A dictionary containing:
            - "bucket": Full bucket name including the environment prefix.
            - "key": Object key/path inside the bucket (location without the original bucket name).
            - "s3_location": Full S3 path in the format 's3a://bucket/key'.

    Examples:
        >>> path_to_s3("data/input/file.csv", "dev")
        {
            'bucket': 'dev-data',
            'key': 'input/file.csv',
            's3_location': 's3a://dev-data/input/file.csv'
        }
    """
    splited_inbound = location.split("/")
    bucket_name = f"{env}-" + splited_inbound[0]
    splited_inbound.pop(0)
    key_inbound = "/".join(splited_inbound)

    s3_inbound_location = f"s3a://{bucket_name}/{key_inbound}"
    return {
        "bucket": bucket_name,
        "key": key_inbound,
        "s3_location": s3_inbound_location,
    }


class S3Process:
    """
    A class to interact with Amazon S3 for file operations such as upload and listing files.

    This class initializes a connection to S3 using provided AWS credentials and endpoint,
    and provides methods to upload files and list objects in a specified bucket.

    Attributes:
        s3_client (boto3.client): A low-level client representing Amazon S3.
    """

    def __init__(self):
        """
        Initialize an S3 client using global AWS credentials and endpoint.

        Uses the following global variables:
            - aws_key: AWS access key ID
            - aws_secret: AWS secret access key
            - aws_endpoint: Custom endpoint URL for S3-compatible services

        Examples:
            >>> s3 = S3Process()
        """
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            endpoint_url=aws_endpoint,
        )

    def s3_raw_file_write(self, file_object, bucket, file_name) -> None:
        """
        Upload a file-like object to an S3 bucket.

        Args:
            file_object (Any): A file-like object to upload. Must be readable and seekable.
            bucket (str): Name of the S3 bucket to upload to.
            file_name (str): The destination key (path/filename) inside the bucket.

        Returns:
            None

        Raises:
            botocore.exceptions.ClientError: If the upload fails due to network issues or permissions.

        Examples:
            >>> with open("local_file.csv", "rb") as f:
            ...     s3.s3_raw_file_write(f, "my-bucket", "path/to/remote_file.csv")
        """

        self.s3_client.upload_fileobj(
            Fileobj=file_object,
            Bucket=bucket,
            Key=file_name,
        )

    def s3_list_files(self, bucket, file_name) -> Union[List[str], bool]:
        """
        List all files under a specific prefix in an S3 bucket.

        Args:
            bucket (str): Name of the S3 bucket to search in.
            file_name (str): Prefix used to filter objects (e.g., directory path or filename pattern).

        Returns:
            list[str] | bool: A list of matching object keys if found; False otherwise.

        Examples:
            >>> s3.s3_list_files("my-bucket", "data/input/")
            ['data/input/file1.csv', 'data/input/file2.csv']
        """
        files = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=file_name).get(
            "Contents"
        )
        if files:
            return [x["Key"] for x in files]
        else:
            return False
