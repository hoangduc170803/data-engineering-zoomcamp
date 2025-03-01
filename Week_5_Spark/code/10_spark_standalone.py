
import pyspark
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder.appName("test").getOrCreate()




spark




df_green = spark.read.parquet(input_green)



df_green.show()




df_green.printSchema()




df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') 




df_yellow = spark.read.parquet(input_yellow)




df_yellow.printSchema()




df_yellow = df_yellow.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') 




common_columns = set(df_green.columns).intersection(set(df_yellow.columns))




common_columns




from pyspark.sql import functions as F




df_green_selected = df_green.select(list(common_columns)).withColumn('service_type',F.lit('green'))





df_yellow_selected = df_yellow.select(list(common_columns)).withColumn('service_type',F.lit('yellow'))





df_trips_data = df_green_selected.union(df_yellow_selected)





df_trips_data.groupBy('service_type').count().show()





df_trips_data.createOrReplaceTempView('trips_data') 





spark.sql("SELECT service_type, count(*) FROM trips_data GROUP BY service_type").show()





df_trips_data.printSchema()




df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")





df_result.show()




df_result.coalesce(1).write.parquet(output, mode='overwrite')






