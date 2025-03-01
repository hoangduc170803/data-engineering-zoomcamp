#Main python file
gs://dtc_data_lake_de-zoomcamp_ny-taxi/code/10_spark_standalone.py

# Argument
--input_green=gs://dtc_data_lake_de-zoomcamp_ny-taxi/pq/green/2021/*/
--input_yellow=gs://dtc_data_lake_de-zoomcamp_ny-taxi/pq/yellow/2021/*/
--output=gs://dtc_data_lake_de-zoomcamp_ny-taxi/report/report-2021



# job submit
$ gcloud dataproc jobs submit pyspark \
    --project=terraform-demo-447612 \
    --cluster=datazoomcamp-cluster \
    --region=asia-southeast1 \
    gs://dtc_data_lake_de-zoomcamp_ny-taxi/code/10_spark_standalone.py \
    -- \
        --input_green=gs://dtc_data_lake_de-zoomcamp_ny-taxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_de-zoomcamp_ny-taxi/pq/yellow/2020/*/ \
        --output=gs://dtc_data_lake_de-zoomcamp_ny-taxi/report/report-2020