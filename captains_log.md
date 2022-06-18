### 6/18/2022
Added function to check is table exists in `bigquery`. This should allow the data workflows to create tables when necessary. I remembered from the testing that the order flow writes to `GCS` and that is where table updates are created, so `WRITE_TRUNCATE` is the correct disposition. I should probably follow my TODO to write the files as partitions for easier checking/management.

### 6/11/2022
Updated `bigquery` function to allow for selecting the `write_disposition`. It defaults to the safest one.

### 5/30/2022 pt2
Completed the migration to public, and moved somethings around to prep for future `Docker` containerization and Github Actions work.

### 5/30/2022
Need to double check, but I am fairly sure that all the private info is out of the repo. It should be good to publish. Final steps are to create a new repo, move the remaining files here to it and create it as a new public repo.

### 5/25/2022
Decided I wanted to make repo public. Goal is to separate configs info into its own private repo, and core functions here can be in public repo.
Refactored `gcs.py` and `bigquery.py` in preparation.
Next steps will be to re-write `README.md`, migrate configs, and the publish new repo.

### 4/10/2022
Fixed issues with products and did another load into BQ. Everything worked from the command line, which means it can be turned into an automated job. Pyarrow was infact not in `requirements.txt`.

Started making progress on orders. It probably needs to be a header and detail table. Wrote some TODOs in `orders.py` for tracking. There is also an outstanding TODO in `shmovement.py` for better failure recovery logic. Also, `shmovement.py` will work for now for orders, but it will be a mess eventually.

### 4/9/2022
Making some progress on orders, but when I ran the products update it is failing to upload from `GCS` to `BQ`. It says that `pyarrow` is not installed in the virtual environment? 
`shmovement` needs an update to pick-up where it leaves off when these failures happen.

### 2/3/2022
Migrated the shopify to bq process to its own library, `shmovement`.

### 1/22/2022
Everything is working in the product workflow now. I completed a number of rewrites in `gcs` and `bigquery` after discovering that `joblib` wasn't really meant for `csv`. The `csv` that flows to `gcs` is basically a point in time view of the product catalog. What get's written into BigQuery still isn't ideal. I want that to be a master list of all products ever and to manage an active status. What it currently is will do for now since with the archived files I can always go back in time.

### Next Up
Generalize the work in `product.py` to a generic utility for orchestrating orders and customers next.

### 1/3/2022
Just jumping in to work on the BigQuery transfer. I didn't get it done. Had to do some `gcs` rewrites, `bigquery` won't load `joblib` files. I also need to figure out how to handle current vs. previous API snapshot. Basically I want to log the unadaultered API result, but put the complete file into BQ. I have the move old file to archive, save new file, but I need to create the current DB file and load it to compare to the new API call.


### 1/1/2022

+ Product flow should be complete from a logic flow standpoint. There are a few TODOs in there.
+ Next up is to write the DB update to BQ.
+ Afterwards, turn the whole product flow into a function.
    + This might require making DB part its own thing. Not every data load will be to BQ, i.e. images

#### Next Up

+ After products, I will be moving to orders and customers.
