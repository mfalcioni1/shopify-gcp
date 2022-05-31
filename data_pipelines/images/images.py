# https://shopify.dev/api/admin-rest/2021-07/resources/product-image#top

# local
import shopify_utils.api as su
import gcp_utils.gcs as gcs
import utils._utils as utils

# third party
import pandas as pd
import numpy as np

## TODO: Write checks/orchestration here.
## TODO: Still not getting the list of objects right
## TODO: Next steps after is to compare the two lists
gcs_uri = f"{utils.load_config()['gcs-product']}images"
image_ids = su.get_image_ids()
gcs_path = gcs.GCSPath(gcs_uri)
stored_imgs = gcs.get_gcs_objects(gcs_path)


su.get_images(image_ids)
