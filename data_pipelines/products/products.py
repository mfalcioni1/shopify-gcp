# https://shopify.dev/api/admin-rest/2021-10/resources/product#top
# from importlib import reload 
# local
import shopify_utils.api as su
import utils._utils as utils
import utils.shmovement as move

def main():
    #TODO: Validation needs to happen if ids=skus or if I have to make another table for SKU mappings
    
    query_params = {'fields': 'id, title, body_html, product_type, handle, created_at, updated_at, \
                    published_at, template_suffix, status, published_scope, tags, admin_graphql_api_id'}
    api_res = su.get_products(query_params)

    move.shop_to_gcs_bq(
        api = "products",
        api_res = api_res,
        gcs_path = f"{utils.load_config()['gcs-product']}product-info",
        dataset = "products",
        table_name = "product_details"
    )

if __name__ == "__main__":
    main()
