
import requests
import json
import datetime
import boto3
import os

# SFMC API endpoint
sfmc_content_blocks_url = "https://<SFMC_instance>.rest.marketingcloudapis.com/asset/v1/content/assets"

# Credentials
sfmc_client_id = os.environ.get("SFMC_CLIENT_ID")
sfmc_client_secret = os.environ.get("SFMC_CLIENT_SECRET")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_s3_bucket = os.environ.get("AWS_S3_BUCKET_NAME")

# Authenticate with SFMC API
def get_last_run_timestamp():
    with open("last_run_timestamp.txt", "r") as file:
        last_run_timestamp = file.read().strip()
    return last_run_timestamp

# Store the last run timestamp in the file
def store_last_run_timestamp(timestamp):
    with open("last_run_timestamp.txt", "w") as file:
        file.write(timestamp)

def authenticate_sfmc_api():
    auth_url = "https://<SFMC_instance>.auth.marketingcloudapis.com/v2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": sfmc_client_id,
        "client_secret": sfmc_client_secret,
        "account_id": "<SFMC_account>"
    }
    response = requests.post(auth_url, data=payload)
    if response.status_code == 200:
        auth_token = response.json()["access_token"]
        return auth_token
    else:
        raise Exception("Failed to authenticate")

# Fetch updated or new content blocks
def fetch_content_blocks(auth_token):
    headers = {
        "Authorization": "Bearer " + auth_token,
        "Content-Type": "application/json"
    }
    # Customize the payload
    payload = {
        "query": {
            "property": "updatedAt",
            "simpleOperator": "greaterThan",
            "value": "<LAST_RUN_TIMESTAMP>"
        }
    }
    response = requests.post(sfmc_content_blocks_url, headers=headers, json=payload)
    if response.status_code == 200:
        content_blocks = response.json()["items"]
        return content_blocks
    else:
        raise Exception("Failed to fetch content blocks.")

# Copy the content blocks into S3
def copy_to_s3(content_blocks):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    # Create a subfolder with the current date in the format "backup_010623"
    subfolder_name = "backup_" + datetime.datetime.now().strftime("%d%m%y")
    for block in content_blocks:
        file_name = block["name"] + ".json"
        file_content = json.dumps(block)
        s3.put_object(Body=file_content, Bucket=aws_s3_bucket, Key=subfolder_name + "/" + file_name)

def main():
    try:
        # Retrieve the last run timestamp
        last_run_timestamp = get_last_run_timestamp()
        # Authenticate
        auth_token = authenticate_sfmc_api()

        # Fetch content
        content_blocks = fetch_content_blocks(auth_token)

        # Copy content into S3
        copy_to_s3(content_blocks)

        # Update the last run timestamp
        last_run_timestamp = datetime.datetime.now().isoformat()

        # Add any additional logic or processing here

    except Exception as e:
        print("Error:", str(e))

if __name__ == "__main__":
    main()
