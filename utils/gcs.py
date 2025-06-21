from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
import os

def upload_file_to_gcs(bucket_name, local_path, destination_blob_name=None):
    """
    Uploads a file from local disk to Google Cloud Storage (GCS).

    Args:
        bucket_name (str): Name of the target GCS bucket.
        local_path (str): Path to the local file to be uploaded.
        destination_blob_name (str, optional): Target name in GCS. Defaults to filename.

    Returns:
        str: GCS path if successful, otherwise raises an exception.
    """
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"❌ File not found: {local_path}")

    destination_blob_name = destination_blob_name or os.path.basename(local_path)

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Ensure bucket exists
        if not bucket.exists():
            raise ValueError(f"❌ GCS bucket '{bucket_name}' does not exist.")

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_path)

        gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
        print(f"✅ Uploaded successfully to {gcs_uri}")
        return gcs_uri

    except GoogleAPIError as e:
        raise RuntimeError(f"💥 GCS upload failed: {e.message}") from e
    except Exception as e:
        raise RuntimeError(f"💥 Unexpected error during GCS upload: {str(e)}") from e
