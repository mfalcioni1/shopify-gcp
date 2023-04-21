# built in
import os
import re
import datetime

# local
import utils._utils as utils

# third party
import gcsfs
import pandas as pd

from google.cloud import storage

if os.getenv("GOOGLE_CLOUD_PROJECT", "") == "":
    utils.gcp_config()

GCS = storage.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT", ""))
GCSFS = gcsfs.GCSFileSystem(project=os.getenv("GOOGLE_CLOUD_PROJECT", ""))

class GCSPath(str):
    """Simplify working with GCS URIs

    Args:
        uri (str): input full GCS path
    Attributes:
        path (str): The underlying folder structure for bucket
        bucket (str): The bucket name
    Example:
        uri = GCSPath("my-bucket/my-folder/my-file.csv")
        print(uri)
        print(uri.path)
        print(uri.bucket)
    """

    def __new__(cls, uri):
        # add in gs://
        uri = re.sub("^[/]*(?!gs://)", "gs://", uri)
        return super().__new__(cls, uri)
    
    def __init__(self, value):
        self.__parts = self[5:].split("/")
        self.__path = "/".join(self.__parts[1:])
        self.__bucket = self.__parts[0]

    # returns the root bucket
    @property
    def bucket(self):
        return self.__bucket

    # returns the path without the root bucket
    @property
    def path(self):
        return self.__path
    
    #return gcspath up a level
    def up(self):
        return GCSPath("gs://" + "/".join(self.__parts[:-1]))
    
    #add to gcs path
    def add(self, value):
        return GCSPath("gs://" + "/".join(self.__parts + [value]))

def get_gcs_objects(
    uri: GCSPath,
    gcs_client: storage.Client = None,
    ) -> list:
    """Outputs a list of files for a given
    uri prefix
    Args:
        uri (GCSPath): GCS URI you want to list objects for
        gcs_client (storage.Client): gcs auth object (optional)
    Return:
        files (list): list of files in bucket
    """
    gcs_client = gcs_client or GCS

    blob = gcs_client.list_blobs(uri.bucket, prefix=uri.path)
    files = []
    for i in blob:
        files.append(i.name)
        
    return files

def mv_blob(uri: GCSPath,
    blob_name: str, 
    destination_uri: GCSPath, 
    destination_blob_name: str, 
    gcs_client: storage.Client = None) -> None:
    """Moves a blob from one bucket to another with a new name.
    Args:
        uri (GCSPath): the bucket URI your original blob is
        blob_name (str): The ID of your GCS object
        destination_uri (GCSPath): the bucket URI you want to move to
        destination_blob_name (str): new name of the blob in new location
        gcs_client (storage.Client): gcs auth object (optional)
    Return:
        None
    Example:
        uri = GCSPath("your-bucket-name")
        blob_name = "your-object-name"
        destination_uri = GCSPath("destination-bucket-name")
        destination_blob_name = "destination-object-name"
        mv_blob(uri, blob_name, destination_uri, destination_blob_name)
    """
    gcs_client = gcs_client or GCS

    assert isinstance(uri, GCSPath), "uri must be a GCSPath"
    assert isinstance(destination_uri, GCSPath), "destination_uri must be a GCSPath"

    source_bucket = gcs_client.get_bucket(uri.bucket)
    source_blob = source_bucket.blob(f"{uri.path}/{blob_name}")
    destination_bucket = gcs_client.get_bucket(destination_uri.bucket)
    destination_blob = f"{destination_uri.path}/{destination_blob_name}"

    # copy to new destination
    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob)
    # delete in old destination
    source_blob.delete()
    
    print(f'File moved from {source_blob} to {destination_blob}')

def upload_to_bucket(uri: GCSPath,
    local_path: str,
    gcs_client: storage.Client = None):
    """Load a file into cloud storage
    Args:
        uri (GCSPath): the bucket URI your original blob is
        local_path (str): location of file locally
        gcs_client (storage.Client): gcs auth object (optional)
    Return:
        success message
    Example:
        upload_to_bucket("my-bucket", "my-folder/my-file.csv")
    """
    gcs_client = gcs_client or GCS

    assert isinstance(uri, GCSPath), "uri must be a GCSPath"

    bucket = gcs_client.get_bucket(uri.bucket)
    blob = bucket.blob(uri.path)
    blob.upload_from_filename(local_path)
    return print(f"Uploaded {local_path} to {uri}")

def pd_to_gcs(df: pd.DataFrame, 
    bucket_uri: GCSPath, 
    bucket: storage.Bucket, 
    file_name: str, 
    archive: bool):
    """Uploads a pandas dataframe to a GCS bucket as a csv
    Args:
        df (pd.DataFrame): dataframe to upload
        bucket_uri (GCSPath): the bucket and path to save the object
        bucket (storage.Bucket): gcs bucket object
        file_name (str): name of the file to save
        archive (bool): whether to write an archive of the file
    Return:
        None
    Example:
        bucket_uri = GCSPath("my-bucket/my-folder")
        bucket = GCS.get_bucket(bucket_uri.bucket)
        file_name = "my-file.csv"
        pd_to_gcs(df, bucket_uri, bucket, file_name, archive=True)
    """
    fp = f"{bucket_uri.path}/{file_name}.csv"
    bucket.blob(fp).upload_from_string(df.to_csv(index=False), "text/csv")
    print("Successfully wrote to: " + fp)
    if archive:
        afp = f"{date_stamp(f'{bucket_uri.path}/archive/{file_name}_')}.csv"
        bucket.blob(afp).upload_from_string(df.to_csv(index=False), "text/csv")
        print("Succesfully archived to: " + afp)

def partition_dataframe_to_gcs(df: pd.DataFrame, 
                               bucket_name, 
                               prefix, 
                               partition_columns, 
                               gcs_client: storage.Client = None):
    
    gcs_client = gcs_client or GCS
    bucket = gcs_client.get_bucket(bucket_name)

    def create_gcs_path(row, prefix, partition_columns):
        path = prefix
        for column in partition_columns:
            path += f"{column}={row[column]}/"
        return path

    df['gcs_path'] = df.apply(lambda row: create_gcs_path(row, prefix, partition_columns), axis=1)

    for path, partition_df in df.groupby('gcs_path'):
        partition_file = path.rstrip('/') + '.csv'
        partition_df.drop(columns=['gcs_path'], inplace=True)
        partition_df.to_csv(partition_file, index=False)
        blob = bucket.blob(partition_file)
        blob.upload_from_filename(partition_file)
        os.remove(partition_file)
        print(f"Uploaded {partition_file} to {bucket_name}/{partition_file}")


def gcs_to_dataframe(uri: GCSPath,
    local_path: str,
    cleanup_local: bool = True,
    gcs_client: storage.Client = None) -> pd.DataFrame:
    """Read file from GCS to pandas DF
    Args:
        uri (GCSPath): the bucket URI your original blob is
            it does accept * as wildcard for shards
        local_path (str): Location to download files
        cleanup_local (bool): to delete local download or not
        gcs_client (storage.Client): gcs auth object (optional)
    """
    
    gcs_client = gcs_client or GCS

    assert isinstance(uri, GCSPath), "uri must be a GCSPath"

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    bucket = gcs_client.get_bucket(uri.bucket)
    blob = bucket.blob(uri.path)
    local_file = f"{local_path}/{blob.name.split('/')[-1]}"
    blob.download_to_filename(local_file)
    df = pd.read_csv(local_file)

    if cleanup_local:
        os.remove(local_file)
        
    return df

def date_stamp(name: str):
    """Adds a date stamp to a file name"""
    utc = datetime.utcnow()
    s_utc = utc.strftime("%Y%m%d")
    return f"{name}{s_utc}"