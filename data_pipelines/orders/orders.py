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
    # first order date: 2018-04-13
    order_detail = su.get_orders(query_params = {'created_at_min': '2023-04-01', 'created_at_max': '2023-04-30'})
    
    billing_address = pd.json_normalize(order_detail['billing_address']).add_prefix('billing_')
    customer = pd.json_normalize(order_detail['customer']).add_prefix('customer_')
    # customer.to_csv('customer.csv')
    #TODO: Anonymize Customer

    order_detail.loc[:, 'order_date'] = order_detail['created_at'].str[:10]

    oh_cols = ['id', 'order_date', 'order_number', 'name', 'source_name', 
        'confirmed', 'financial_status', 'fulfillment_status',
        'created_at', 'processed_at', 'closed_at',
        'subtotal_price', 'total_price', 'total_tax', 'current_total_price', 
        'total_discounts', 'discount_codes',
        'note', 'note_attributes', 'cancel_reason', 'updated_at', 'cancelled_at']
    order_header  = order_detail[oh_cols]
    # change created_at to order_date and remove time stamp

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

    # creating order_detail table
    od_cols = ['id', 'order_date', 'cancel_reason', 'cancelled_at', 'closed_at', 'confirmed', 
               'created_at', 'current_subtotal_price', 'current_total_discounts', 
               'current_total_price', 'current_total_tax', 'discount_codes', 'email', 
               'financial_status', 'fulfillment_status', 'landing_site', 'landing_site_ref', 
               'name', 'note', 'order_number', 'processed_at', 'referring_site', 'source_name',
               'subtotal_price', 'tags', 'total_discounts', 'total_line_items_price',
               'total_outstanding', 'total_price', 'total_tax',
               'total_weight', 'updated_at', 'user_id', 'discount_applications', 'line_items',
               'refunds']
    # TODO: Parse line items to get to product level
    if bq.bq_table_exists(dataset_id = "orders", table_name = "order_detail"):
        od_write_disposition = "WRITE_TRUNCATE"
    else:
        od_write_disposition = "WRITE_EMPTY"

    od_gcs_path = gcs.GCSPath(f"{utils.load_config()['gcs-orders']}order-detail")

    od_move = move.shmover(api="order_detail",
                           api_res=order_detail[od_cols],
                           gcs_path=od_gcs_path,
                           dataset="orders",
                           table_name="order_detail",
                           write_disposition=od_write_disposition,
                           partition=True,
                           partition_by="order_date")
    
    od_move.shop_to_gcs()
    od_move.gcs_partition_to_bq()

    line_items = pd.concat([pd.json_normalize(x) for x in order_detail['line_items']])
    line_items.reset_index(drop=True, inplace=True)

    line_items = su.expand_nested_col(df=order_detail, 
                                   col='line_items', 
                                   id_col='id',
                                   id_col_name='order_id')
if __name__ == "__main__":
    main()

# main()

### Scratch work ###

# testing how we can make an API request from the most recent data
# next_last_id = order_detail['id'].iloc[-2]
# test = su.get_orders(query_params = {'since_id' : next_last_id})
# last_id = order_detail['id'].iloc[-1]
# su.get_orders(query_params = {'since_id' : last_id}) #produces error :(