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
