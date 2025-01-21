import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def download_file_from_spaces(bucket_name, object_key, destination_path):
    try:
        # Fetch credentials from environment variables
        secret_key = os.getenv('DIGITALOCEAN_SPACES_TOKEN')
        access_key = os.getenv('DIGITAL_OCEAN_SPACES_ID')

        if not access_key or not secret_key:
            raise EnvironmentError("SPACES_ACCESS_KEY or SPACES_SECRET_KEY environment variables are not set.")

        # Configure the S3 client
        session = boto3.session.Session()
        client = session.client(
            's3',
            region_name='nyc3',  # Change region if required
            endpoint_url='https://nyc3.digitaloceanspaces.com',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        # Download the file
        print(f"Downloading {object_key} from {bucket_name} to {destination_path}...")
        client.download_file(bucket_name, object_key, destination_path)
        print(f"File downloaded successfully to {destination_path}")

    except NoCredentialsError:
        print("Credentials not available.")
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
    except EnvironmentError as e:
        print(f"Environment Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Example usage
if __name__ == "__main__":
    bucket_name = "bagels"
    object_key = "dev/creds/.env"  # The file path in your bucket
    destination_path = "./config/.env"  # Local path where the file will be saved

    # Ensure the destination directory exists
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

    # Download the file
    download_file_from_spaces(bucket_name, object_key, destination_path)
