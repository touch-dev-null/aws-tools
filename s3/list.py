import boto3
import sys

def list_s3_objects(bucket_name=None):
    s3 = boto3.client('s3')

    if bucket_name:
        # List objects in the specified bucket
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get('Contents', []):
                print(f"s3://{bucket_name}/{obj['Key']}")
    else:
        # List all buckets
        response = s3.list_buckets()
        for bucket in response['Buckets']:
            print(f"s3://{bucket['Name']}/")

def main():
    if len(sys.argv) > 1:
        bucket_name = sys.argv[1]
        print(f"Listing objects in bucket: {bucket_name}")
        list_s3_objects(bucket_name)
    else:
        print("Listing all buckets:")
        list_s3_objects()

if __name__ == "__main__":
    main()
