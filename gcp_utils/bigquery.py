# base
import os
from typing import List

# 3rd party
import google.cloud.bigquery as bigquery
import pandas as pd
from google.cloud import bigquery_storage_v1


BQ_CLIENT = bigquery.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT", None))
BQ_READ_CLIENT = bigquery_storage_v1.BigQueryReadClient()

def df_to_bq_table(
    df: pd.DataFrame,
    dataset_id: str,
    table_name: str,
    schema: List[bigquery.SchemaField] = None,
    bq_client: bigquery.Client = None,
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
    # TODO: Allow for more write_dispositions

    bq_client = bq_client or BQ_CLIENT
    table_id = f"{dataset_id}.{table_name}"

    if schema is None:
        print("Auto-detecting schema")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
    else: 
        print("Using provided schema.")
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE"
        )
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()

    table = bq_client.get_table(table_id)
    print(
        f"Loaded {table.num_rows} rows and "
        + f"{len(table.schema)} columns to {table_id}.\n\n"
    )