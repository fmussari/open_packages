##################################
##### 1. IMPORTING LIBRARIES #####
##################################

from google.cloud import storage
from pathlib import Path


#### Cloud Storage API ####


def get_service_account_client(
	JSON_FILE: Path
	) -> storage.client.Client:
	"""Client from JSON filepath"""
	return storage.Client.from_service_account_json(JSON_FILE)


def list_blobs(
        storage_client: storage.client.Client,
        bucket_name:str
    ):
    """Lists all the blobs in the bucket."""
    #storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)
    return list(blobs)

def download_blob(
        storage_client: storage.client.Client, 
        bucket_name: str, 
        source_blob_name: str, 
        destination_file_name: Path
        ) -> bool:

    """If exists, downloads a blob from bucket"""
    bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name)
    
    blob = bucket.blob(source_blob_name)
    #print('blob.exists()', blob.exists())

    if blob.exists():
        blob.download_to_filename(destination_file_name)
        #print('blob', blob)
        return True
    return False

def upload_blob(
        storage_client: storage.client.Client, 
        bucket_name: str, 
        source_file_name: Path, 
        destination_blob_name: str
        ) -> None:

    """Uploads a file to the bucket"""
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(str(source_file_name))

    file_name = source_file_name.stem + source_file_name.suffix
    print('File {} uploaded to {}'.format(
        file_name,
        destination_blob_name))

def patch_blob_metadata(
        storage_client: storage.client.Client,
        bucket_name: str, 
        blob_name: str, 
        metadata: dict
        ) -> None:

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    print('blob', blob)
    blob.metadata = metadata
    blob.patch()