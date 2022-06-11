# first party
import os

# third party
import pandas as pd

# local
import shopify_utils.api as su
import gcp_utils.gcs as gcs
import gcp_utils.bigquery as bq
import utils._utils as utils

def shop_to_gcs_bq(
    api: str,
    api_res: pd.DataFrame,
    gcs_path: str,
    dataset: str,
    table_name: str,
    write_disposition: str = None
    ):
    """Take API result store/archive it into GCS, and then place into BQ table.
    Args
        api (string): the api from which the data came i.e. products
        api_res (pd.DataFrame): pandas dataframe of data to be loaded into gcs and BQ
        gcs_path (string): path to store the new api result and archive last data
        dataset (string): dataset name for where to place the data
        table_name (string): name of the bq table to export the data
    Returns
        print of the results of the load.
    """

    api_res = api_res.fillna('') #nans/nones get messy when reading in and out; TODO: Is this right?
    # check if new table is necessary.
    files = gcs.get_gcs_objects(gcs.GCSPath(gcs_path))
    latest_api, latest_db, update_bool = utils.check_latest(api_res, files)
    if update_bool:
        file_name = f"{utils.date_stamp(f'{api}')}.csv"
        api_res.to_csv(path_or_buf=f"./{file_name}")
        gcs.upload_to_bucket(
            gcs.GCSPath(f"{gcs_path}/{file_name}"),
            f"{os.getcwd()}/{file_name}")
        #TODO: This probably needs fleshed out some to treat orders differently
        # for example, maybe export files by a day partition?
        if latest_db:

            old_file = f"{api}{latest_db}.csv"
            db_gcs = gcs.gcs_to_dataframe(gcs.GCSPath(f"{gcs_path}/{old_file}"),
                os.getcwd()).fillna('').set_index('id')

            gcs.mv_blob(gcs.GCSPath(gcs_path), old_file, gcs.GCSPath(f"{gcs_path}/archive"), old_file)
            db_gcs.update(api_res, join='left')
            db_update = pd.concat([db_gcs, api_res], join="outer").drop_duplicates()
            # TODO: If this step fails mid-moving the files to BQ, then it sticks in a limbo state
            # where the cloud storage is updated but the BQ table is not. Re-running results in the 
            # process not performing any updates since the checks are storage exclusive
            bq.df_to_bq_table(df = db_update,
            dataset_id = dataset,
            table_name = table_name,
            write_disposition = write_disposition
            )
        else:
            print("No previous files to compare.")
        return print(f"Latest {api} added as of {latest_api}.")
    else:
        return print(f"No new {api} found. No DB updates performed")
