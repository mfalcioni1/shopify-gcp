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
    order_detail = su.get_orders(query_params = {'since_id' : 4722290491636})
    #TODO: Need to come up with an order list update flow.
    #TODO: Need to parse out the order header
    #TODO: Need to parse out the order detail
    order_detail.head()
    order_detail.columns
    order_detail.shape
    test_case = order_detail[order_detail['id'] == 4722290491636] #best example
    tc_res = pd.json_normalize(order_detail['billing_address'])
    oh_cols = [['id', 'order_number', 'name', 'source_name', 
        'confirmed', 'processing_method', 'financial_status', 'fulfillment_status',
        'created_at', 'processed_at', 'closed_at',
        'subtotal_price', 'total_price', 'total_tax', 'current_total_price', 
        'total_discounts', 'discount_codes',
        'note', 'note_attributes', 'cancel_reason', 'updated_at', 'cancelled_at']]
    od_cols = [['id', 'order_number', 'name', 'source_name', ]]


if __name__ == "__main__":
    main()

# main()

### Scratch work ###
# 5823 is a really good order example row
for i in range(len(order_detail.columns)):
    print(f'{order_detail.columns[i]}: {order_detail.iloc[5823, i]}')


# testing how we can make an API request from the most recent data
next_last_id = order_detail['id'].iloc[-2]
test = su.get_orders(query_params = {'since_id' : next_last_id})
last_id = order_detail['id'].iloc[-1]
su.get_orders(query_params = {'since_id' : last_id}) #produces error :(