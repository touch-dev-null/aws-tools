import boto3
import logging
import sys

from botocore.exceptions import ClientError

logging.basicConfig(filename='success.log', level=logging.INFO)
error_log = logging.getLogger('error')
error_log.addHandler(logging.FileHandler('error.log'))

def delete_files_from_s3(file_paths):
    """
    Deletes files from S3 and logs actions.
    """
    s3 = boto3.client('s3')
    for file_path in file_paths:
        bucket_name, key = file_path.split('/', 2)[-1].split('/', 1)
        try:
            s3.head_object(Bucket=bucket_name, Key=key)

            response = s3.list_object_versions(Bucket=bucket_name, Prefix=key)

            # Delete all versions of the object
            for version in response.get('Versions', []):
                print(f"Key: {version['Key']}, VersionId: {version['VersionId']}")

                if version['Key'] != key:
                    sys.exit()

                s3.delete_object(Bucket=bucket_name, Key=key, VersionId=version['VersionId'])

            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"Going to delete Bucket={bucket_name}, Key={key} \n")
            logging.info(f"Deleted file: {file_path}")

        except Exception as e:
            error_log.error(f"Failed to delete file {file_path}: {e}")

def read_file_list(file_path):
    """
    Reads file list from a text file.
    """
    with open(file_path, 'r') as file:
        file_list = file.readlines()
    return [line.strip() for line in file_list]

def write_file_list(file_path, file_list):
    """
    Writes file list to a text file.
    """
    with open(file_path, 'w') as file:
        file.write('\n'.join(file_list))

def main():
    file_list_path = 'files-to-remove.txt'
    file_list = read_file_list(file_list_path)

    # Delete files from S3
    delete_files_from_s3(file_list)

    # Remove processed paths from file list
    write_file_list(file_list_path, [])


if __name__ == "__main__":
    main()

