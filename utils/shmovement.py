# first party
import os
import datetime
import re

# third party
import pandas as pd

# local
import shopify_utils.api as su
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
        gcs_path: str,
        dataset: str,
        table_name: str,
        write_disposition: str = None,
        partition: bool = False,
        parition_by: str = "order_date"):
            self.api = api
            self.api_res = api_res
            self.gcs_path = gcs_path
            self.dataset = dataset
            self.table_name = table_name
            self.write_disposition = write_disposition
            self.partition = partition
            self.partition_by = self.partition_by

    def date_stamp(self, name: str):
        utc = datetime.datetime.utcnow()
        s_utc = utc.strftime("%Y%m%d")
        self.file_date_stamp = f"{name}{s_utc}"

    def check_latest(self, api_res, gcs_iter):
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

    def remove_nas(self):
        """Cleanup of empties in api res
        """
        self.api_res = self.api_res.fillna('')

    def shop_to_gcs(self):
        """Take an API result, and store/archive it into GCS.
        """
        self.remove_nas()
        files = gcs.get_gcs_objects(gcs.GCSPath(self.gcs_path))
        self.check_latest(self.api_res, files)
        if self.update_bool & self.api not in "order":
            file_name = f"{self.date_stamp(f'{self.api}')}.csv"
            self.api_res.to_csv(path_or_buf=f"./data/{file_name}")
            gcs.upload_to_bucket(
                gcs.GCSPath(f"{self.gcs_path}/{file_name}"),
                f"{os.getcwd()}/{file_name}")
        elif self.update_bool & self.api in "order": ## You are here
            partition_list = self.api_res[self.parition_by].unique()
            #TODO: This probably needs fleshed out some to treat orders differently
            # for example, maybe export files by a day partition?

            # latest_file = [x for x in gcs_iter if str(self.latest_db) in x]
            # latest_orders = gcs.gcs_to_dataframe(gcs.GCSPath(f"{self.gcs_path}{latest_file[0]}"), 
            #     os.getcwd).fillna('').set_index('id')

def shop_to_gcs_bq(self
    ):
    """Take API result store/archive it into GCS, and then place into BQ table.
    Args

    Returns
        print of the results of the load.
    """

     #nans/nones get messy when reading in and out; TODO: Is this right?
    # check if new table is necessary.

        if latest_db:

            old_file = f"{self.api}{latest_db}.csv"
            db_gcs = gcs.gcs_to_dataframe(gcs.GCSPath(f"{self.gcs_path}/{old_file}"),
                os.getcwd()).fillna('').set_index('id')

            gcs.mv_blob(gcs.GCSPath(self.gcs_path), old_file, gcs.GCSPath(f"{self.gcs_path}/archive"), old_file)
            db_gcs.update(self.api_res, join='left')
            db_update = pd.concat([db_gcs, self.api_res], join="outer").drop_duplicates()
            # TODO: If this step fails mid-moving the files to BQ, then it sticks in a limbo state
            # where the cloud storage is updated but the BQ table is not. Re-running results in the 
            # process not performing any updates since the checks are storage exclusive
            bq.df_to_bq_table(df = db_update,
            dataset_id = self.dataset,
            table_name = self.table_name,
            write_disposition = self.write_disposition
            )
        else:
            print("No previous files to compare.")
        return print(f"Latest {self.api} added as of {latest_api}.")
    else:
        return print(f"No new {self.api} found. No DB updates performed")
