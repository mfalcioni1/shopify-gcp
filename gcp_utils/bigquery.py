# base
import os
from typing import List
from time import sleep
import subprocess

# local
from gcp_utils import gcs

# 3rd party
import google.cloud.bigquery as bigquery
import pandas as pd


BQ_CLIENT = bigquery.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT", None))

def bq_table_exists(
    dataset_id: str,
    table_name: str,
    bq_client: bigquery.Client = None
) -> bool:
    """Helper to check if a table exists or not.
    Args:
        dataset_id (str): dataset to check for table
        table_name (str): table name to check for
        bq_client (bigquery.Client): BQ auth (optional)
    Return:
        tbl_exists (bool): T/F if table already exists or not.
    """
    bq_client = bq_client or BQ_CLIENT

    tables = bq_client.list_tables(dataset_id)
    
    tbl_exists = False

    for table in tables:
        if "{}.{}".format(dataset_id, table_name) == "{}.{}".format(table.dataset_id, table.table_id):
            tbl_exists = True

    return tbl_exists

def df_to_bq_table(
    df: pd.DataFrame,
    dataset_id: str,
    table_name: str,
    schema: List[bigquery.SchemaField] = None,
    write_disposition = None,
    bq_client: bigquery.Client = None
) -> None:
    """Helper to write DF to table.
    Args:
        df (pd.DataFrame): A pandas dataframe to send to BQ.
        dataset_id (str): dataset to send table
        table_name (str): table name to place table in dataset
        schema (List[bigquery.SchemaField]): table schema (optional)
        bq_client (bigquery.Client): BQ auth (optional)
    Return:
        None
    """

    bq_client = bq_client or BQ_CLIENT
    table_id = f"{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig()

    if write_disposition is None:
        print("No disposition provided, write empty being used.")
        job_config.write_disposition = "WRITE_EMPTY"
    else:
        print(f"Using {write_disposition} as write disposition")
        job_config.write_disposition = write_disposition

    if schema is None:
        print("Auto-detecting schema")
    else: 
        print("Using provided schema.")
        job_config.schema = schema

    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()

    table = bq_client.get_table(table_id)
    print(
        f"Loaded {table.num_rows} rows and "
        + f"{len(table.schema)} columns to {table_id}.\n\n"
    )
    
def gcs_partition_to_bq(bucket_uri: gcs.GCSPath,
    table: str, 
    replace: str,
    project_id: str = None,
    files_to_load: str = None,
    partition_uri_prefix: gcs.GCSPath = None,
    schema: dict = None,
    bq_client: bigquery.Client = None):
    if partition_uri_prefix is None:
        partition_uri_prefix = bucket_uri
    if schema is None:
        schema = "--autodetect"
    else:
        schema = f"--schema={schema}"
    
    bq_client = bq_client or BQ_CLIENT
    project_id = project_id or bq_client.project

    if files_to_load is None:
        bq_load = ["bq", "load", f"--replace={str(replace).lower()}", 
                   "--source_format=CSV", "--skip_leading_rows=1", 
                   schema, "--hive_partitioning_mode=AUTO",
                   f"--project_id={project_id}",
                   f"--hive_partitioning_source_uri_prefix={partition_uri_prefix}",
                   f"{table}", f"{partition_uri_prefix}/*"]
    else:
        bq_load = ["bq", "load", f"--replace={str(replace).lower()}", 
                   "--source_format=CSV", "--skip_leading_rows=1", 
                   schema, "--hive_partitioning_mode=AUTO",
                   f"--project_id={project_id}",
                   f"--hive_partitioning_source_uri_prefix={partition_uri_prefix}",
                   f"{table}", f"{files_to_load}"]
    print(f"Loading with {bq_load}")
    try:
        output = subprocess.check_output(bq_load)
        print(output)
    except subprocess.CalledProcessError as e:
        print(e.output)
        raise e
    print("Big Query Load Successful.")
