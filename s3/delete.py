import boto3
import os
from botocore.exceptions import ClientError
import argparse

def delete_s3_object(s3_client, bucket_name, object_key, dry_run=False):
    try:
        # List all versions of the object
        paginator = s3_client.get_paginator('list_object_versions')
        versions = {'Versions': [], 'DeleteMarkers': []}
        for page in paginator.paginate(Bucket=bucket_name, Prefix=object_key):
            if 'Versions' in page:
                versions['Versions'].extend(page['Versions'])
            if 'DeleteMarkers' in page:
                versions['DeleteMarkers'].extend(page['DeleteMarkers'])
        objects_to_delete = []
        if 'Versions' in versions:
            objects_to_delete.extend([{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in versions['Versions']])
        if 'DeleteMarkers' in versions:
            objects_to_delete.extend([{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in versions['DeleteMarkers']])

        if objects_to_delete:
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i+1000]
                if dry_run:
                    print(f"[DRY RUN] Would delete {len(batch)} versions of object: s3://{bucket_name}/{object_key}")
                    for obj in batch:
                        print(f"  - Would delete version: {obj['VersionId']}")
                else:
                    s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': batch})
                    print(f"Deleted {len(batch)} versions of object: s3://{bucket_name}/{object_key}")
                    for obj in batch:
                        print(f"  - Deleted version: {obj['VersionId']}")
        else:
            if dry_run:
                print(f"[DRY RUN] Would delete object: s3://{bucket_name}/{object_key}")
            else:
                s3_client.delete_object(Bucket=bucket_name, Key=object_key)
                print(f"Deleted object: s3://{bucket_name}/{object_key}")
        return True
    except ClientError as e:
        print(f"Error {'simulating deletion of' if dry_run else 'deleting'} object s3://{bucket_name}/{object_key}: {e}")
        return False

def delete_s3_bucket(s3_client, bucket_name, dry_run=False):
    try:
        # List all objects and versions in the bucket
        paginator = s3_client.get_paginator('list_object_versions')
        deleted_count = 0
        for page in paginator.paginate(Bucket=bucket_name):
            objects_to_delete = []
            if 'Versions' in page:
                objects_to_delete.extend([{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in page['Versions']])
            if 'DeleteMarkers' in page:
                objects_to_delete.extend([{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in page['DeleteMarkers']])

            if objects_to_delete:
                # Delete objects in batches of 1000 (AWS limit)
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i+1000]
                    if dry_run:
                        print(f"[DRY RUN] Would delete {len(batch)} objects/versions from bucket: s3://{bucket_name}")
                        for obj in batch:
                            print(f"  - Would delete: {obj['Key']} (Version: {obj['VersionId']})")
                    else:
                        s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': batch})
                        print(f"Deleted {len(batch)} objects/versions from bucket: s3://{bucket_name}")
                        for obj in batch:
                            print(f"  - Deleted: {obj['Key']} (Version: {obj['VersionId']})")
                    deleted_count += len(batch)

        # Check if the bucket is empty before deleting
        remaining_objects = list(s3_client.list_objects(Bucket=bucket_name, MaxKeys=1).get('Contents', []))
        if not remaining_objects:
            # Only delete the bucket if it's the root bucket (no '/' in the name)
            if '/' not in bucket_name:
                if dry_run:
                    print(f"[DRY RUN] Would delete root bucket: s3://{bucket_name}")
                else:
                    s3_client.delete_bucket(Bucket=bucket_name)
                    print(f"Deleted root bucket: s3://{bucket_name}")
            else:
                print(f"{'[DRY RUN] Would clear' if dry_run else 'Cleared'} all objects from: s3://{bucket_name}")
            print(f"Total objects/versions {'that would be' if dry_run else ''} deleted: {deleted_count}")
            return True
        else:
            print(f"Bucket s3://{bucket_name} still contains objects. Please retry the operation.")
            return False
    except ClientError as e:
        print(f"Error {'simulating processing of' if dry_run else 'processing'} bucket s3://{bucket_name}: {e}")
        return False

def process_s3_link(s3_client, link, dry_run=False):
    parts = link.replace('s3://', '').strip('/').split('/')
    bucket_name = parts[0]
    object_key = '/'.join(parts[1:]) if len(parts) > 1 else ''

    if object_key:
        return delete_s3_object(s3_client, bucket_name, object_key, dry_run)
    else:
        return delete_s3_bucket(s3_client, bucket_name, dry_run)

def main():
    parser = argparse.ArgumentParser(description='Delete S3 objects and buckets.')
    parser.add_argument('--dry-run', action='store_true', help='Simulate deletion without actually deleting')
    args = parser.parse_args()

    s3_client = boto3.client('s3')
    input_file = 'objects-to-remove.txt'
    error_log = 'delete-error-log.txt'

    with open(input_file, 'r') as f:
        links = f.readlines()

    successful_deletions = []

    for link in links:
        link = link.strip()
        print(f"\n{'[DRY RUN] Simulating processing' if args.dry_run else 'Processing'}: {link}")
        if process_s3_link(s3_client, link, args.dry_run):
            successful_deletions.append(link)
            print(f"Successfully {'simulated' if args.dry_run else 'processed'}: {link}")
        else:
            with open(error_log, 'a') as error_file:
                error_file.write(f"Failed to {'simulate' if args.dry_run else 'process'}: {link}\n")
            print(f"Failed to {'simulate' if args.dry_run else 'process'}: {link}")

    if not args.dry_run:
        # Remove successfully processed objects from the input file
        with open(input_file, 'w') as f:
            for link in links:
                if link.strip() not in successful_deletions:
                    f.write(link)

    print(f"\nSummary:")
    print(f"Total links processed: {len(links)}")
    print(f"Successfully {'simulated' if args.dry_run else 'processed'}: {len(successful_deletions)}")
    print(f"Failed operations: {len(links) - len(successful_deletions)}")

if __name__ == "__main__":
    main()
