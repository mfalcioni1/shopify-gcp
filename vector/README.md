## Set-up
https://codelabs.developers.google.com/codelabs/alloydb-ai-embedding
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

```sh
gcloud services vpc-peerings connect \
    --service=servicenetworking.googleapis.com \
    --ranges=psa-range \
    --network=default
```

Create the cluster:
```sh
export PGPASSWORD=`openssl rand -base64 12`
export REGION=us-central1
export ADBCLUSTER=alloydb-aip-01
gcloud alloydb clusters create $ADBCLUSTER \
    --password=$PGPASSWORD \
    --network=default \
    --region=$REGION
```

Create a primary instance:
```sh
export REGION=us-central1
gcloud alloydb instances create $ADBCLUSTER-pr \
    --instance-type=PRIMARY \
    --cpu-count=2 \
    --region=$REGION \
    --cluster=$ADBCLUSTER
```

Create a VM
```sh
export ZONE=us-central1-a
gcloud compute instances create $ADBCLUSTER-vm \
    --zone=$ZONE \
    --scopes=https://www.googleapis.com/auth/cloud-platform
gcloud compute ssh $ADBCLUSTER-vm --zone=us-central1-a
```

Install postgres
```sh
sudo apt-get update
sudo apt-get install --yes postgresql-client
```

Set-up env
```sh
gcloud config set project perrico-plant-co-dev
export REGION=us-central1
export ADBCLUSTER=alloydb-aip-01
#return DB IP
gcloud alloydb instances describe $ADBCLUSTER-pr --cluster=$ADBCLUSTER --region=$REGION --format="(ipAddress)"
export PGPASSWORD={PW_FROM_ABOVE}
export INSTANCE_IP={IP_FROM_ABOVE}
```

Connect to DB
```sh
#connect to DB
psql "host=$INSTANCE_IP user=postgres sslmode=require"
```