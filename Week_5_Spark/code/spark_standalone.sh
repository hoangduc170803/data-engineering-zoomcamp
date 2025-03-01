 .\spark-class.cmd org.apache.spark.deploy.master.Master --host localhost --port 7077 --webui-port 8080

spark-class.cmd org.apache.spark.deploy.worker.Worker spark://localhost:7077

 jupyter nbconvert --to=script 10_spark_standalone.ipynb

spark-submit.cmd --master spark://localhost:7077 Week_5_Spark\code\10_spark_standalone.py

spark-submit \
    --master="spark://<URL>" \
    my_script.py \
        --input_green=data/pq/green/2020/*/ \
        --input_yellow=data/pq/yellow/2020/*/ \
        --output=data/report-2020


spark-submit \
    --master="spark://localhost:7077" \
    10_spark_standalone.py \
        --input_green=data/pq/green/2020/*/ \
        --input_yellow=data/pq/yellow/2020/*/ \
        --output=data/report-2020        

#Window
spark-submit.cmd --master spark://localhost:7077 10_spark_standalone.py --input_green=data/pq/green/2020/*/ --input_yellow=data/pq/yellow/2020/*/ --output=data/report/report-2020

spark-submit.cmd --master spark://localhost:7077 10_spark_standalone.py --input_green=data/pq/green/2021/*/ --input_yellow=data/pq/yellow/2021/*/ --output=data/report/report-2021

$URL = "spark://localhost:7077"
spark-submit.cmd --master $URL 10_spark_standalone.py --input_green=data/pq/green/2021/*/ --input_yellow=data/pq/yellow/2021/*/ --output=data/report/report-2021


#Stop master and worker
sbin/stop-master.sh
sbin/stop-worker.sh 
