# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This notebook sets up the environment for the delta demo.  It is assumed that you have already setup an Azure Storage Account with a container named *demos* and secured the account key in a Databricks Secret named for the storage account. It also assumes that you have downloaded some number of months' worth of [trip data](https://s3.amazonaws.com/tripdata/index.html) from the NYC Citibikes program, unzipped it and placed this in a folder named /citibikenyc/csv within your Azure Storage Account container.  (Details on the structure of this data can be found [here](https://www.citibikenyc.com/system-data).)

# COMMAND ----------

# mount storage location

# check to see if not already mounted:
if not (u'/mnt/demos' in map( lambda m: m.mountPoint, dbutils.fs.mounts() )):

  # if not, then mount now using secret key
  dbutils.fs.mount(
    source = 'wasbs://demos@brysmiwasb.blob.core.windows.net',
    mount_point = '/mnt/demos',
    extra_configs = {
      'fs.azure.account.key.brysmiwasb.blob.core.windows.net':
      dbutils.secrets.get(scope = 'storage', key = 'brysmiwasb')
      }
    )

# COMMAND ----------

csv_file_path = 'dbfs:/mnt/demos/citibikenyc/csv/'
parquet_file_path = 'dbfs:/mnt/demos/citibikenyc/parquet/'
delta_file_path = 'dbfs:/mnt/demos/citibikenyc/delta/'

# COMMAND ----------

# verify expected files are in place
expected_csv_file_count = 13
actual_csv_file_count = len(dbutils.fs.ls(csv_file_path))

if actual_csv_file_count != expected_csv_file_count:
  raise BaseException('Expected {0} files in {1}. Found {2}.'.format(expected_csv_file_count, csv_file_path, actual_csv_file_count))

# COMMAND ----------

# MAGIC %md With files in place, drop the demo database (named citibikenyc) and recreate it as an empty database:

# COMMAND ----------

# re-create delta demo database
spark.sql('drop database if exists citibikenyc cascade')
spark.sql('create database citibikenyc')

# make sure folders are clean
dbutils.fs.rm(parquet_file_path, recurse=True)
dbutils.fs.rm(delta_file_path, recurse=True)

# COMMAND ----------

# MAGIC %md Read data from the CSV trip data files into a dataframe:

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import to_date, hour

csv_schema = StructType([
  StructField('tripduration', IntegerType()),
  StructField('starttime', TimestampType()),
  StructField('stoptime', TimestampType()),
  StructField('start_station_id', IntegerType()),
  StructField('start_station_name', StringType()),
  StructField('start_station_latitude', FloatType()),
  StructField('start_station_longitude', FloatType()),
  StructField('end_station_id', IntegerType()),
  StructField('end_station_name', StringType()),
  StructField('end_station_latitude', FloatType()),
  StructField('end_station_longitude', FloatType()),
  StructField('bikeid', IntegerType()),
  StructField('usertype', StringType()),
  StructField('birth_year', IntegerType()),
  StructField('gender', StringType())  
  ])

# repartition assumes 3 workers with 4 vcores / worker
# and is intended to product messy data distribution
df = spark.read.csv(csv_file_path, header=True, schema=csv_schema, quote='"')\
  .repartition(12, 'start_station_id')\
  .withColumn('startdate', to_date('starttime'))\
  .withColumn('starthour', hour('starttime')).orderBy('startdate', 'starthour')\
  .cache()

display(df)

# COMMAND ----------

# MAGIC %md Load the data into a parquet table in a manner that creates fragmentation:

# COMMAND ----------

# push data to partitioned parquet table on 
# hourly basis to create fragmentation
for i in range(0,24):
  print('Hour {0}'.format(i))
  df.where(df.starthour==i).select(df.columns[:-1]).write\
    .saveAsTable(
      'citibikenyc.trips',
      format='parquet',
      mode='append',
      partitionBy='startdate',
      path = parquet_file_path
      )

# COMMAND ----------

# MAGIC %md Load data into a delta table in the same manner:

# COMMAND ----------

# push data to partitioned delta table on 
# hourly basis to create fragmentation
for i in range(0,24):
  print('Hour {0}'.format(i))
  df.where(df.starthour==i).select(df.columns[:-1]).write\
    .saveAsTable(
      'citibikenyc.trips_delta',
      format='delta',
      mode='append',
      partitionBy='startdate',
      path = delta_file_path
      )