#!/bin/bash

echo "Hello You are copying the data..."

echo "Hope you are in clusterdata-2011-2 folder..."

mkdir job_events task_events machine_events machine_attributes task_constraints task_usage

#gsutil ls gs://clusterdata-2011-2/job_events
gsutil cp gs://clusterdata-2011-2/job_events/part-00000-of-00500.csv.gz ./job_events/

#gsutil ls gs://clusterdata-2011-2/task_events
gsutil cp gs://clusterdata-2011-2/task_events/part-00000-of-00500.csv.gz ./task_events/

#gsutil ls gs://clusterdata-2011-2/machine_events
gsutil cp gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz ./machine_events/

#gsutil ls gs://clusterdata-2011-2/machine_attributes
gsutil cp gs://clusterdata-2011-2/machine_attributes/part-00000-of-00001.csv.gz ./machine_attributes/

#gsutil ls gs://clusterdata-2011-2/task_constraints
gsutil cp gs://clusterdata-2011-2/task_constraints/part-00000-of-00500.csv.gz ./task_constraints/

#gsutil ls gs://clusterdata-2011-2/task_usage
gsutil cp gs://clusterdata-2011-2/task_usage/part-00000-of-00500.csv.gz ./task_usage/

#gcloud dataproc jobs submit pyspark gcp.py \
    --cluster=cluster-8c02 \
    --region=europe-west2 \
    -- gs://bucket-large-scale-project/input/ gs://bucket-large-scale-project/output/


gcloud dataproc jobs submit pyspark cloud_performance.py     --cluster=cluster-8c02 --region=europe-west2

PROJECT=eastern-crawler-337619

BUCKET_NAME=bucket-large-scale-project

CLUSTER=cluster-8c02

REGION=europe-west2

gcloud dataproc clusters create ${CLUSTER} \
    --project=${PROJECT} \
    --region=${REGION} \
    --single-node

gsutil cp -r gs://clusterdata-2011-2 gs://${BUCKET_NAME}/clusterdata-2011-2

https://console.cloud.google.com/storage/browser/bucket-large-scale-project;tab=objects?project=eastern-crawler-337619&prefix=&forceOnObjectsSortingFiltering=false