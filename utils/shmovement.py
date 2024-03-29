# first party
import os
import datetime
import re

# third party
import pandas as pd

# local
import gcp_utils.gcs as gcs
import gcp_utils.bigquery as bq

class shmover:
    """
    Attributes
        api (string): the api from which the data came i.e. products
        api_res (pd.DataFrame): pandas dataframe of data to be loaded into gcs and BQ
        gcs_path (string): path to store the new api result and archive last data
        dataset (string): dataset name for where to place the data
        table_name (string): name of the bq table to export the data
        write_disposition (string): 
            WRITE_TRUNCATE: If the table already exists, BigQuery overwrites 
                the table data and uses the schema from the query result.
            WRITE_APPEND: If the table already exists, BigQuery appends 
                the data to the table.
            WRITE_EMPTY: If the table already exists and contains data, a 
                'duplicate' error is returned in the job result.
    """
    def __init__(self, 
        api: str,
        api_res: pd.DataFrame,
        gcs_path: gcs.GCSPath,
        dataset: str,
        table_name: str,
        write_disposition: str = None,
        partition: bool = False,
        partition_by: str = "order_date"):
            self.api = api
            self.api_res = api_res
            self.bucket_uri = gcs_path
            self.bucket = gcs.GCS.get_bucket(self.bucket_uri.bucket)
            self.dataset = dataset
            self.table_name = table_name
            self.write_disposition = write_disposition
            self.partition = partition
            self.partition_by = partition_by

    def _check_latest(self, api_res, gcs_iter):
        newest = api_res[api_res['created_at'] == api_res['created_at'].max()]['created_at'].iloc[0]
        self.latest_api = int(datetime.datetime.fromisoformat(newest).strftime("%Y%m%d"))
        if gcs_iter:
            r = re.compile("\d+")
            self.latest_db = max([int(r.search(x).group(0)) for x in gcs_iter])
            self.update_bool = self.latest_api > self.latest_db       
        else:
            print("No files in GCS. Setting update to True.")
            self.latest_db = None
            self.update_bool = True

    def _remove_nas(self):
        """Cleanup of empties in api res
        """
        self.api_res = self.api_res.fillna('')

    def shop_to_gcs(self):
        """Take an API result, and store/archive it into GCS.
        """
        self._remove_nas()
        files = gcs.get_gcs_objects(self.bucket_uri)
        self._check_latest(self.api_res, files)
        file_name = f"{self.api}.csv"
        if self.update_bool and not self.partition:
            gcs.pd_to_gcs(df=self.api_res, bucket_uri=self.bucket_uri, 
                          file_name=file_name, archive=True)
            
        elif self.update_bool and self.partition:
            files = []
            for n, g in self.api_res.groupby(pd.Grouper(self.partition_by)):
                fn = self.bucket_uri.path + "/" + "date=" + n + "/" + file_name
                print(f"Writing to: {fn}")
                files.append("gs://" + self.bucket_uri.bucket + "/" + fn)
                self.bucket.blob(fn).upload_from_string(g.drop(self.partition_by, axis=1).to_csv(index=False), "text/csv")
    
    #TODO: Flow for writing non-partitioned files to BQ
    def gcs_partition_to_bq(self):
        """Take GCS Hive partitioned files and load into BQ.
        """

        if self.update_bool:
            replace = not "WRITE_APPEND" in self.write_disposition
            bq.gcs_partition_to_bq(bucket_uri=self.bucket_uri,
                table=f"{self.dataset}.{self.table_name}",
                replace=replace,
                partition_uri_prefix=self.bucket_uri)
            return print(f"Latest {self.api} added as of {self.latest_api}.")
        else:
            return print(f"No new data to add. Latest {self.api} added as of {self.latest_db}.")

    def shop_to_bq(self):
        """Take API result and place into BQ table.
        Args

        Returns
            print of the results of the load.
        """
        bq.df_to_bq_table(df = self.api_res,
            dataset_id = self.dataset,
            table_name = self.table_name,
            write_disposition = self.write_disposition
        )
        return print(f"Latest {self.api} added as of {self.latest_api}.")