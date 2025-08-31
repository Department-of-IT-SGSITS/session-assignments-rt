import boto3
import hashlib

# Initialize S3 client
s3 = boto3.client('s3')

BUCKET_NAME = "removeredundancys3"

def get_file_hash(s3, bucket, key):
    """Download object and compute SHA256 hash"""
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read()
    return hashlib.sha256(data).hexdigest()

def deduplicate(bucket):
    seen_hashes = {}
    duplicates = []

    response = s3.list_objects_v2(Bucket=bucket)
    if 'Contents' not in response:
        print("Bucket is empty!")
        return

    for item in response['Contents']:
        key = item['Key']
        file_hash = get_file_hash(s3, bucket, key)

        if file_hash in seen_hashes:
            duplicates.append(key)
        else:
            seen_hashes[file_hash] = key

    print("\nDuplicate files found:")
    for dup in duplicates:
        print(f"- {dup}")
        # Uncomment below to actually delete
        s3.delete_object(Bucket=bucket, Key=dup)

if __name__ == "__main__":
    deduplicate(BUCKET_NAME)
