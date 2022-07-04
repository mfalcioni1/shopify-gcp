# https://shopify.dev/api/admin-rest/2021-07/resources/order#top
# from importlib import reload 
# first party
import os

# third party
import pandas as pd

# local
import shopify_utils.api as su
import gcp_utils.gcs as gcs
import gcp_utils.bigquery as bq
import utils._utils as utils
import utils.shmovement as move

def main():
    #order_detail = su.get_orders()
    order_detail = su.get_orders(query_params = {'created_at_min': '2022-05-01', 'created_at_max': '2022-06-01'})
    #TODO: Need to come up with an order list update flow.
    #TODO: Need to parse out the order header
    #TODO: Need to parse out the order detail
    order_detail.head()
    order_detail.columns
    order_detail.shape

    order_detail['order_date'] = pd.to_datetime(order_header['created_at'])
    order_detail['order_date'] = order_detail['order_date'].dt.date
    # unnesting json columns
    # for i in order_detail.columns:
    #     if order_detail[i].dtype == "object"
    billing_address = pd.json_normalize(order_detail['billing_address']).add_prefix('billing_')
    customer = pd.json_normalize(order_detail['customer']).add_prefix('customer_')
    #TODO: Anonymize Customer

    oh_cols = ['id', 'order_number', 'order_date', 'name', 'source_name', 
        'confirmed', 'processing_method', 'financial_status', 'fulfillment_status',
        'created_at', 'processed_at', 'closed_at',
        'subtotal_price', 'total_price', 'total_tax', 'current_total_price', 
        'total_discounts', 'discount_codes',
        'note', 'note_attributes', 'cancel_reason', 'updated_at', 'cancelled_at']
    order_header  = order_detail[oh_cols]

    if bq.bq_table_exists(dataset_id = "orders", table_name = "order_header"):
        oh_write_disposition = "WRITE_TRUNCATE"
    else:
        oh_write_disposition = "WRITE_EMPTY"
    
    move.shop_to_gcs_bq(
            api = "order_header",
            api_res = order_header,
            gcs_path = f"{utils.load_config()['gcs-orders']}order-header",
            dataset = "orders",
            table_name = "order_header",
            write_disposition = oh_write_disposition,
            partition = True,
            partition_by = "created_at")


if __name__ == "__main__":
    main()

# main()

### Scratch work ###

# testing how we can make an API request from the most recent data
next_last_id = order_detail['id'].iloc[-2]
test = su.get_orders(query_params = {'since_id' : next_last_id})
last_id = order_detail['id'].iloc[-1]
su.get_orders(query_params = {'since_id' : last_id}) #produces error :(