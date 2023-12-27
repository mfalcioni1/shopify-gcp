## Set-up

```sh
gcloud config set compute/region us-central1
gcloud services enable aiplatform.googleapis.com
gcloud services enable alloydb.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable servicenetworking.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
```

```sh
gcloud compute addresses create psa-range \
    --global \
    --purpose=VPC_PEERING \
    --prefix-length=16 \
    --description="VPC private service access" \
    --network=default
```