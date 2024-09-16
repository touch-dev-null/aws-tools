import boto3
import os
from botocore.exceptions import ClientError

def delete_s3_object(s3_client, bucket_name, object_key):
    try:
        # Delete all versions of the object
        versions = s3_client.list_object_versions(Bucket=bucket_name, Prefix=object_key)
        objects_to_delete = []
        if 'Versions' in versions:
            objects_to_delete.extend([{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in versions['Versions']])
        if 'DeleteMarkers' in versions:
            objects_to_delete.extend([{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in versions['DeleteMarkers']])

        if objects_to_delete:
            s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
            print(f"Deleted {len(objects_to_delete)} versions of object: s3://{bucket_name}/{object_key}")
            for obj in objects_to_delete:
                print(f"  - Deleted version: {obj['VersionId']}")
        else:
            s3_client.delete_object(Bucket=bucket_name, Key=object_key)
            print(f"Deleted object: s3://{bucket_name}/{object_key}")
        return True
    except ClientError as e:
        print(f"Error deleting object s3://{bucket_name}/{object_key}: {e}")
        return False

def delete_s3_bucket(s3_client, bucket_name):
    try:
        # Delete all objects and versions in the bucket
        paginator = s3_client.get_paginator('list_object_versions')
        deleted_count = 0
        for page in paginator.paginate(Bucket=bucket_name):
            objects_to_delete = []
            if 'Versions' in page:
                objects_to_delete.extend([{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in page['Versions']])
            if 'DeleteMarkers' in page:
                objects_to_delete.extend([{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in page['DeleteMarkers']])

            if objects_to_delete:
                s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
                deleted_count += len(objects_to_delete)
                print(f"Deleted {len(objects_to_delete)} objects/versions from bucket: s3://{bucket_name}")
                for obj in objects_to_delete:
                    print(f"  - Deleted: {obj['Key']} (Version: {obj['VersionId']})")

        # Delete the bucket
        s3_client.delete_bucket(Bucket=bucket_name)
        print(f"Deleted bucket: s3://{bucket_name}")
        print(f"Total objects/versions deleted: {deleted_count}")
        return True
    except ClientError as e:
        print(f"Error deleting bucket s3://{bucket_name}: {e}")
        return False

def process_s3_link(s3_client, link):
    parts = link.replace('s3://', '').strip('/').split('/')
    bucket_name = parts[0]
    object_key = '/'.join(parts[1:]) if len(parts) > 1 else ''

    if object_key:
        return delete_s3_object(s3_client, bucket_name, object_key)
    else:
        return delete_s3_bucket(s3_client, bucket_name)

def main():
    s3_client = boto3.client('s3')
    input_file = 'objects-to-remove.txt'
    error_log = 'delete-error-log.txt'

    with open(input_file, 'r') as f:
        links = f.readlines()

    successful_deletions = []

    for link in links:
        link = link.strip()
        print(f"\nProcessing: {link}")
        if process_s3_link(s3_client, link):
            successful_deletions.append(link)
            print(f"Successfully deleted: {link}")
        else:
            with open(error_log, 'a') as error_file:
                error_file.write(f"Failed to delete: {link}\n")
            print(f"Failed to delete: {link}")

    # Remove successfully deleted objects from the input file
    with open(input_file, 'w') as f:
        for link in links:
            if link.strip() not in successful_deletions:
                f.write(link)

    print(f"\nSummary:")
    print(f"Total links processed: {len(links)}")
    print(f"Successfully deleted: {len(successful_deletions)}")
    print(f"Failed deletions: {len(links) - len(successful_deletions)}")

if __name__ == "__main__":
    main()
