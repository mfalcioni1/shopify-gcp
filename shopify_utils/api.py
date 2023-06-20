# base
import os

# local
import utils._utils as utils

# third party
from urllib.parse import urlencode
import requests
import pandas as pd
import wget

# TODO: Write more sophisticated client class

config = utils.load_config()

def build_url(api_params: dict, query_params: dict = None):
    version = init_api()
    api_params = {**version, **api_params}
    
    url = ('https://{apikey}:{password}@{hostname}/admin/api/'.format(**config) + 
    '{version}/{resource}.json'.format(**api_params))
    if query_params != None:
        url = url + '?' + urlencode(query_params)   

    return url

def init_api():
    version = {'version' : config['api-version']}
    return version

def api_iter(api_params, query_params):
    df=pd.DataFrame()
    
    # Initialize parameters
    if query_params == None:
        query_params = {'limit' : 250, 'since_id' : 0}
    elif 'since_id' in query_params:
        query_params['limit'] = 250
    else:
        query_params['limit'] = 250
        query_params['since_id'] = 0
    
    api = api_params['resource']

    # loop through the API responses, max is 250
    while True:
        url = build_url(api_params, query_params)
        response = requests.request('GET', url)
        
        res = pd.DataFrame(response.json()[api])
        df = pd.concat([df,res])
        query_params['since_id'] = df['id'].iloc[-1]
        if len(res) < 250:
            break
    return df

def get_products(query_params = None):
    api_params = {'resource' : 'products'}
    products = api_iter(api_params, query_params).set_index('id')
    return products

def get_orders(query_params = None):
    api_params = {'resource': 'orders'}
    if query_params:
        query_params['status'] = 'any'
    else:
        query_params = {'status': 'any'}
    orders = api_iter(api_params, query_params)
    return(orders)

def get_image_ids(query_params = None):
    prd = api_iter({'resource' : 'products'}, 
                    {'fields' : 'id,images'})
    xplode = prd['images'].explode().reset_index(level=0)
    xplode = xplode.where(pd.notnull(xplode), None)
    image_ids = pd.DataFrame({'image_id' : pd.Series(dtype='int'), 
                            'product_id': pd.Series(dtype='int'), 
                            'image_src': pd.Series(dtype='str')})
    i=0
    for row in xplode['images']:
        if row != None:
            df = pd.DataFrame({'image_id' : row['id'], 
                            'product_id' : row['product_id'],
                            'image_src' : row['src']}, index = [i])
            image_ids = image_ids.append(df)
            i += 1
    return image_ids

def get_images(image_ids: pd.DataFrame):
    for index, row in image_ids.iterrows():
            index_1 = index + 1
            img = f'images/{row["image_id"]}_{row["product_id"]}.jpg'
            img_url = row['image_src']
            wget.download(img_url, img)
    return print(f'{index_1} images written')

def expand_nested_col(df: pd.DataFrame, 
                      col: str, 
                      id_col: str,
                      id_col_name: str) -> pd.DataFrame:
    # Create new dataframe
    new_data = []

    # For each row in the DataFrame
    for idx, row in df.iterrows():
        # Use json_normalize on the 'col' column and add the 'id' column
        for item in row[col]:
            temp = pd.json_normalize(item)
            temp[id_col_name] = row[id_col]
            new_data.append(temp)

    # Concatenate all dataframes in the list
    new_df = pd.concat(new_data)
    new_df.reset_index(drop=True, inplace=True)
    return new_df