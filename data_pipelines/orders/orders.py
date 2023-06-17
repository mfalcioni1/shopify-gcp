# https://shopify.dev/api/admin-rest/2021-07/resources/order#top
# from importlib import reload 
# first party
import sys
sys.path.append('C:/Users/Matt/Documents/Projects/shopify-gcp')

# third party
import pandas as pd

# local
import shopify_utils.api as su
import gcp_utils.gcs as gcs
import gcp_utils.bigquery as bq
import utils._utils as utils
import utils.shmovement as move

def main():
    order_detail = su.get_orders(query_params = {'created_at_min': '2022-05-01', 'created_at_max': '2022-06-01'})
    
    billing_address = pd.json_normalize(order_detail['billing_address']).add_prefix('billing_')
    customer = pd.json_normalize(order_detail['customer']).add_prefix('customer_')
    # customer.to_csv('customer.csv')
    #TODO: Anonymize Customer

    oh_cols = ['id', 'order_number', 'name', 'source_name', 
        'confirmed', 'financial_status', 'fulfillment_status',
        'created_at', 'processed_at', 'closed_at',
        'subtotal_price', 'total_price', 'total_tax', 'current_total_price', 
        'total_discounts', 'discount_codes',
        'note', 'note_attributes', 'cancel_reason', 'updated_at', 'cancelled_at']
    order_header  = order_detail[oh_cols]
    # change created_at to order_date and remove time stamp
    order_header.loc[:, 'order_date'] = order_header['created_at'].str[:10]

    if bq.bq_table_exists(dataset_id = "orders", table_name = "order_header"):
        oh_write_disposition = "WRITE_TRUNCATE"
    else:
        oh_write_disposition = "WRITE_EMPTY"
    
    oh_gcs_path = gcs.GCSPath(f"{utils.load_config()['gcs-orders']}order-header")

    oh_move = move.shmover(api="order_header",
        api_res=order_header,
        gcs_path=oh_gcs_path,
        dataset="orders",
        table_name="order_header",
        write_disposition=oh_write_disposition,
        partition=True,
        partition_by="order_date")
    
    oh_move.shop_to_gcs()
    oh_move.gcs_partition_to_bq()

if __name__ == "__main__":
    main()

# main()

### Scratch work ###

# testing how we can make an API request from the most recent data
# next_last_id = order_detail['id'].iloc[-2]
# test = su.get_orders(query_params = {'since_id' : next_last_id})
# last_id = order_detail['id'].iloc[-1]
# su.get_orders(query_params = {'since_id' : last_id}) #produces error :(