import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col
from graphframes import *

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Task 1
    # Subtask 1
    # Loading mini data (for quicker testing and debugging)
    # Reading CSV files using pyspark.sql.SparkSession.read.csv() and setting to inferSchema to False since assignment requires resulting schema to be with string data types and not int
    # rideshare_data = spark.read.csv("s3a://" + s3_bucket + "/sample_data.csv", sep=',', inferSchema=False, header=True)
    # taxi_zone_lookup = spark.read.csv("s3a://" + s3_bucket + "/taxi_zone_lookup.csv", sep=',', inferSchema=False, header=True)

    # Loading full data (for correct results for assignment)
    # Reading CSV files using pyspark.sql.SparkSession.read.csv() and setting to inferSchema to False since assignment requires resulting schema to be with string data types and not int
    rideshare_data = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv", sep=',', inferSchema=False, header=True)
    taxi_zone_lookup = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv", sep=',', inferSchema=False, header=True)
    # Printing some lines from both data frames
    rideshare_data.show(5)
    taxi_zone_lookup.show(5)    
    
    # Subtask 2
    # Joining rideshare_data with taxi_zone lookup. Requires two joins. Joins done using pyspark.sql.DataFrame.join()
        # Join 1: join on columns pickup_location of rideshare_data and LocationID of taxi_zone_lookup
        # Join 2: join on columns dropoff_location of rideshare_data and LocationID of taxi_zone_lookup
    # Redundant column LocationID is dropped after both joins using pyspark.sql.DataFrame.drop()
    # Renaming columns after first join using pyspark.sql.DataFrame.withColumnRenamed()
    # Renaming columns after second join using pyspark.sql.DataFrame.withColumnRenamed()
    task1 = rideshare_data\
                    .join(taxi_zone_lookup, rideshare_data.pickup_location == taxi_zone_lookup.LocationID)\
                    .drop('LocationID')\
                    .withColumnRenamed('Borough', 'Pickup_Borough')\
                    .withColumnRenamed('Zone', 'Pickup_Zone')\
                    .withColumnRenamed('service_zone', 'Pickup_service_zone')\
                    .join(taxi_zone_lookup, rideshare_data.dropoff_location == taxi_zone_lookup.LocationID)\
                    .drop('LocationID')\
                    .withColumnRenamed('Borough', 'Dropoff_Borough')\
                    .withColumnRenamed('Zone', 'Dropoff_Zone')\
                    .withColumnRenamed('service_zone', 'Dropoff_service_zone')

    # printing column names using  using pyspark.sql.DataFrame.columns
    print("NEW COLUMNS: ", task1.columns)
    # 'business', 'pickup_location', 'dropoff_location', 'trip_length', 'request_to_pickup', 'total_ride_time', 'on_scene_to_pickup', 'on_scene_to_dropoff', 'time_of_day', 'date', 'passenger_fare', 'driver_total_pay', 'rideshare_profit', 'hourly_rate', 'dollars_per_mile', 'Pickup_Borough', 'Pickup_Zone', 'Pickup_service_zone', 'Dropoff_Borough', 'Dropoff_Zone', 'Dropoff_service_zone']
    # Pickup_Borough, Pickup_Zone, Pickup_service_zone , Dropoff_Borough, Dropoff_Zone, Dropoff_service_zone

    # Subtask 3
    # Changing format of date column using the following functions from pyspark.sql.functions
        # Using from_unixtime to convert from seconds from UNIX epoch to the system's timezone and format
        # using date_format to convert result from the above to yyyy-MM-dd
    # The formatted date column is used to update the date column using pyspark.sql.DataFrame.withColumn()
    task1.show(5)
    task1 = task1.withColumn('date', date_format(from_unixtime('date'), 'yyyy-MM-dd'))
    task1.show(5)

    df = task1

    # Subtask 4
    # Printing number of rows using pyspark.sql.DataFrame.count()
    print("NUMBER OF ROWS: ", task1.count())
    print("SCHEMA")
    # Printing number of rows using pyspark.sql.DataFrame.printSchema()
    task1.printSchema()

    # Task 2
    # Subtasks 1, 2, and 3
    # Extracting substring with month in date column and making it the new date column
        # The functions used for this are pyspark.sql.DataFrame.withColumn() and pyspark.sql.functions.substring()
    # Renaming date column as month
    # Grouping dataframe by business and month columns using pyspark.sql.DataFrame.groupBy()
    # Applying three aggregate functions using pyspark.sql.DataFrame.agg()
        # count of rows in each group using pyspark.sql.functions.count()
        # sum of rideshare_profit column in each group using pyspark.sql.functions.sum()
        # sum of driver_total_pay column in each group using pyspark.sql.functions.sum()
    # Using pyspark.sql.Column.alias() function for assigning new names to results of aggregate functions
    task2 = df.withColumn('date', F.substring('date', 6, 2))\
            .withColumnRenamed('date', 'month')\
            .groupBy('business', 'month')\
            .agg(F.count("*").alias("trip_counts"),
                 F.sum("rideshare_profit").alias("platform_profits"),
                 F.sum("driver_total_pay").alias("driver_earnings"))
    # To create a column business-month pyspark.sql.functions.concat_ws() has been used with withColumn()
    # Redundant columns business and month have been dropped using drop(), their info is in col business-month
    task2 = task2.withColumn('business-month', F.concat_ws('-', 'business', 'month'))\
                .drop('business')\
                .drop('month')
    task2.show()
    # We export a single file using pyspark.sql.DataFrame.coalesce() and *.write() with option()
    task2.coalesce(1).write.option("header", True).option("delimiter",",").csv("s3a://" + s3_bucket + "/task2/")

    # Task 3
    # Subtask 1
    # Grouping is done on pickup borough and month to give a monthly breakdown of trip counts for each borough
    # Ordering has been done using pyspark.sql.DataFrame.orderBy() with pyspark.sql.functions.desc()
    st1 = df.withColumn('date', F.substring('date', 6, 2))\
            .withColumnRenamed('date', 'Month')\
            .groupBy('Pickup_Borough', 'Month')\
            .agg(F.count("*").alias('trip_count'))\
            .orderBy(F.desc('trip_count'))\
            .show()

    # Subtask 2
    # Grouping is done on dropoff borough and month to give a monthly breakdown of trip counts for each borough
    # Ordering has been done using pyspark.sql.DataFrame.orderBy() with pyspark.sql.functions.desc()
    st2 = df.withColumn('date', F.substring('date', 6, 2))\
            .withColumnRenamed('date', 'Month')\
            .groupBy('Dropoff_Borough', 'Month')\
            .agg(F.count("*").alias('trip_count'))\
            .orderBy(F.desc('trip_count'))\
            .show()

    # Subtask 3
    # Route column is created using concat_ws
    # Grouping has been done on this column to get data for every route
    # Ordering has been done using pyspark.sql.DataFrame.orderBy() with pyspark.sql.functions.desc()
    st3 = df.withColumn('Route', F.concat_ws(' to ', 'Pickup_Borough', 'Dropoff_Borough'))\
            .groupBy('Route')\
            .agg(F.sum('driver_total_pay').alias('total_profit'))\
            .orderBy(F.desc('total_profit'))\
            .show()
    
    # Task 4
    # Subtask 1
    # A grouping has been done on time_of_day
    # Average has been calculated using pyspark.sql.functions.avg() with agg()
    # Ordering has been done using orderBy() and desc()
    st1 = df.groupBy('time_of_day')\
            .agg(F.avg('driver_total_pay').alias("average_drive_total_pay"))\
            .orderBy(F.desc('average_drive_total_pay'))
    st1.show()

    # Subtask 2
    # A grouping has been done on time_of_day
    # Average has been calculated using pyspark.sql.functions.avg() with agg()
    # Ordering has been done using orderBy() and desc()
    st2 = df.groupBy('time_of_day')\
            .agg(F.avg('trip_length').alias("average_trip_length"))\
            .orderBy(F.desc('average_trip_length'))
    st2.show()

    # Subtask 3
    # A join has been done on time_of_day between the above two dataframes
    # Average has been calculated by dividing average pay with average trip length in withColumn()
    # Ordering has been done using orderBy() and desc()
    st3 = st1.join(st2, st1.time_of_day == st2.time_of_day)
    st3 = st3.withColumn('average_earning_per_mile', st3.average_drive_total_pay / st3.average_trip_length)\
            .drop('average_drive_total_pay').drop('average_trip_length')\
            .orderBy(F.desc('average_earning_per_mile'))
    st3.show()
    
    # Task 4
    # Subtask 1
    # A grouping has been done on time_of_day
    # Average has been calculated using pyspark.sql.functions.avg() with agg()
    # Ordering has been done using orderBy() and desc()
    st1 = df.groupBy('time_of_day')\
            .agg(F.avg('driver_total_pay').alias("average_drive_total_pay"))\
            .orderBy(F.desc('average_drive_total_pay'))
    st1.show()

    # Subtask 2
    # A grouping has been done on time_of_day
    # Average has been calculated using pyspark.sql.functions.avg() with agg()
    # Ordering has been done using orderBy() and desc()
    st2 = df.groupBy('time_of_day')\
            .agg(F.avg('trip_length').alias("average_trip_length"))\
            .orderBy(F.desc('average_trip_length'))
    st2.show()

    # Subtask 3
    # A join has been done on time_of_day between the above two dataframes
    # Average has been calculated by dividing average pay with average trip length in withColumn()
    # Ordering has been done using orderBy() and desc()
    st3 = st1.join(st2, st1.time_of_day == st2.time_of_day)
    st3 = st3.withColumn('average_earning_per_mile', st3.average_drive_total_pay / st3.average_trip_length)\
            .drop('average_drive_total_pay').drop('average_trip_length')\
            .orderBy(F.desc('average_earning_per_mile'))
    st3.show()
    
    # Task 5
    # Subtask 1
    # Data has been filtered using pyspark.sql.DataFrame.where()
    task5 = df.withColumn('month', F.substring('date', 6, 2))\
            .where('month = 01')\
            .withColumn('day', F.substring('date', 9, 2))\
            .groupBy('day')\
            .agg(F.avg('request_to_pickup').alias('average_waiting_time'))
    task5.show()

    task5.coalesce(1).write.option("header", True).option("delimiter",",").csv("s3a://" + s3_bucket + "/task5/")

    # Subtask 2
    # Data has been filtered using pyspark.sql.DataFrame.where()
    task5 = task5.where('average_waiting_time > 300').show()
    
    # Task 6
    # Subtask 1
    task6 = df.groupBy('Pickup_Borough', 'time_of_day')\
                .agg(count('*').alias('trip_count'))\
                .where("trip_count > 0")\
                .where("trip_count < 1000")
    task6.show()
    # Subtask 2
    task6 = df.where("time_of_day = 'evening'")\
                .groupBy('Pickup_Borough', 'time_of_day')\
                .agg(count('*').alias('trip_count'))
    task6.show()

    # Subtask 3
    task6 = df.where("Pickup_Borough = 'Brooklyn'")\
                .where("Dropoff_Borough = 'Staten Island'")
    print("Number of trips from Brooklyn to Staten Island: ", task6.count())
    task6.select('Pickup_Borough', 'Dropoff_Borough', 'Pickup_Zone').show(10)
    
    # Task 7
    # Subtask 1
    task7 = df.withColumn('Route', F.concat_ws(' to ', 'Pickup_Borough', 'Dropoff_Borough'))
    task7 = task7.withColumn('Uber', F.when(task7.business == 'Uber', 1).otherwise(0))\
                    .withColumn('Lyft', F.when(task7.business == 'Lyft', 1).otherwise(0))
    task7 = task7.groupBy('Route')\
                    .agg(F.sum('Uber').alias('uber_count'),
                         F.sum('Lyft').alias('lyft_count'),
                         F.count('*').alias('total_count'))\
                    .orderBy(F.desc('total_count'))
    task7.show(10)
    
    spark.stop()