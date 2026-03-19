import boto3
from os import getenv
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import json
from hashlib import md5
from time import localtime

load_dotenv()


def init_client():
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=getenv("aws_access_key_id"),
            aws_secret_access_key=getenv("aws_secret_access_key"),
            aws_session_token=getenv("aws_session_token"),
            region_name=getenv("aws_region_name"))
        # Check if credentials are correct
        client.list_buckets()

        return client
    except ClientError as e:
        print(e)
        raise e


def list_buckets(aws_s3_client):
    try:
        return aws_s3_client.list_buckets()
    except ClientError as e:
        print(e)
        return False


def create_bucket(aws_s3_client, bucket_name,
                  region="us-west-2"):
    try:
        location = {'LocationConstraint': region}
        response = aws_s3_client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration=location)
    except ClientError as e:
        print(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def delete_bucket(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def bucket_exists(aws_s3_client, bucket_name):
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def download_file_and_upload_to_s3(aws_s3_client,
                                   bucket_name,
                                   url,
                                   file_name,
                                   keep_local=False):
    from urllib.request import urlopen
    import io
    with urlopen(url) as response:
        content = response.read()
        try:
            aws_s3_client.upload_fileobj(
                Fileobj=io.BytesIO(content),
                Bucket=bucket_name,
                ExtraArgs={'ContentType': 'image/jpg'},
                Key=file_name)
        except Exception as e:
            print(e)

    if keep_local:
        with open(file_name, mode='wb') as file:
            file.write(content)

    return f"https://s3-us-west-2.amazonaws.com/{bucket_name}/{file_name}"


def set_object_access_policy(aws_s3_client, bucket_name, file_name):
    try:
        response = aws_s3_client.put_object_acl(ACL="public-read",
                                                Bucket=bucket_name,
                                                Key=file_name)
    except ClientError as e:
        print(e)
        return False
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def generate_public_read_policy(bucket_name):
    policy = {
        "Version":
        "2012-10-17",
        "Statement": [{
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": f"arn:aws:s3:::{bucket_name}/*",
        }],
    }

    return json.dumps(policy)


def create_bucket_policy(aws_s3_client, bucket_name):
    aws_s3_client.delete_public_access_block(Bucket=bucket_name)
    aws_s3_client.put_bucket_policy(
        Bucket=bucket_name, Policy=generate_public_read_policy(bucket_name))
    print("Bucket policy created successfully")


def read_bucket_policy(aws_s3_client, bucket_name):
    try:
        policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)
        policy_str = policy["Policy"]
        print(policy_str)
    except ClientError as e:
        print(e)
        return False


if __name__ == "__main__":
    s3_client = init_client()
    print(s3_client)

    s3_name = f'btu-bucket-{md5(str(localtime()).encode("utf-8")).hexdigest()}'
    print(s3_name)
    print(f'created bucket status: { create_bucket(s3_client, s3_name)}')

    create_bucket_policy(s3_client, s3_name)
    read_bucket_policy(s3_client, s3_name)

    # Example usage
    print(
        download_file_and_upload_to_s3(
            s3_client,
            s3_name,
            'https://www.coreldraw.com/static/cdgs/images/free-trials/img-ui-cdgsx.jpg',
            f'image_file_{md5(str(localtime()).encode("utf-8")).hexdigest()}.jpg',
            keep_local=True)
    )

    # buckets = list_buckets(s3_client)
    # if buckets:
    #     for bucket in buckets['Buckets']:
    #         print(f'  {bucket["Name"]}')

    # print(f'deleted bucket status: { delete_bucket(s3_client, "btudevopsteam1")}')
    # print(f'Bucket exists: { bucket_exists(s3_client, "automatinawsbttu-commandline")}')

    # print(f"set read status: {set_object_access_policy(s3_client, 'new-bucket-btu', 'image_file_78bc222b20d1ff69cdf1290a753