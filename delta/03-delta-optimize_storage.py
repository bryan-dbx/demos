# Databricks notebook source
# MAGIC %md With all the manipulations we performed on our delta table, it's good to note that we still have the same number of records as when we originally loaded the data:

# COMMAND ----------

# MAGIC %sql REFRESH TABLE citibikenyc.trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select 'trips', count(*) as trips
# MAGIC from citibikenyc.trips
# MAGIC UNION ALL
# MAGIC select 'trips_delta', count(*) as trips
# MAGIC from citibikenyc.trips_delta;

# COMMAND ----------

# MAGIC %md While the row counts are the same, the file counts are not:
# MAGIC 
# MAGIC **NOTE**: If you limited the "bad etl" and rollback portions of the last exercise to Jan 1, 2019, the difference in file counts will be small.

# COMMAND ----------

# DELTA TABLE
# count parquet files in partition directories (delta table)
n = 0
for i, dir in enumerate(filter( lambda d: d.name[-1]=='/', dbutils.fs.ls('dbfs:/mnt/demos/citibikenyc/delta/'))):
  if dir.name != '_delta_log':
    n += len( filter(lambda p: p.name[-15:]=='.snappy.parquet', dbutils.fs.ls(dir.path)) )

i = i - 1  # exclude the transaction log folder

print('On the delta table, there are {0} files across {1} partition directories for an average of {2}'.format(n, i, float(n)/float(i)))


# STANDARD TABLE
# count parquet files in partition directories (standard table)
n = 0
for i, dir in enumerate(filter( lambda d: d.name[-1]=='/', dbutils.fs.ls('dbfs:/mnt/demos/citibikenyc/parquet/'))):
    n += len( filter(lambda p: p.name[-15:]=='.snappy.parquet', dbutils.fs.ls(dir.path)) )

print('On the standard table, there are {0} files across {1} partition directories for an average of {2} files per partition'.format(n, i, float(n)/float(i)))

# COMMAND ----------

# MAGIC %md As you can see, there are many more files in the delta table compared to the parquet table.  This is because we've been making numerous changes to the data and versioning keeps point in time representations for us.  
# MAGIC 
# MAGIC If we want to keep our versioning information, then we are more or less stuck with the number of files we see here.  We can issue the OPTIMIZE statement to better ensure files are of a more consistent size which is helpful.  Also with the OPTIMIZE statement, we can reorder our data in the files so that it aligns with critical queries:

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE citibikenyc.trips_delta -- WHERE startdate >= '2019-01-01'
# MAGIC    ZORDER BY (bikeid)
# MAGIC    
# MAGIC -- Disable/enable the WHERE clause based on whether you enabled/disabled it with the "bad etl" and rollback cells in the last exercise

# COMMAND ----------

# count parquet files in partition directories (delta table)
n = 0
for i, dir in enumerate(filter( lambda d: d.name[-1]=='/', dbutils.fs.ls('dbfs:/mnt/demos/citibikenyc/delta/'))):
  if dir.name != '_delta_log':
    n += len( filter(lambda p: p.name[-15:]=='.snappy.parquet', dbutils.fs.ls(dir.path)) )

i = i - 1  # exclude the transaction log folder

print('On the delta table, there are {0} files across {1} partition directories for an average of {2} files per partition'.format(n, i, float(n)/float(i)))

# COMMAND ----------

# MAGIC %md All of this helps, but to address our file fragmentation issues, we need to expire versioning information from our delta transaction log.  We can do this with the VACUUM statement.  
# MAGIC 
# MAGIC The VACUUM statement allows us to release versioning information on files older than a specified period of time.  Those files can be conslidated once the versioning info is gone.  By default, delta blocks you from specifying a period of time that's less than 7 days from the current time.  This is to allow long-running transactions to complete and to ensure consistent views of data given various caching capabilities at play within Databricks.  To go below this threshold risks instability, corruption, etc.  So it's not recommended to do what we're showing here unless you know your system is quiesced: 

# COMMAND ----------

spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', False)
spark.sql('VACUUM citibikenyc.trips_delta RETAIN 0 HOURS')

# COMMAND ----------

# count parquet files in partition directories (delta table)
n = 0
for i, dir in enumerate(filter( lambda d: d.name[-1]=='/', dbutils.fs.ls('dbfs:/mnt/demos/citibikenyc/delta/'))):
  if dir.name != '_delta_log':
    n += len( filter(lambda p: p.name[-15:]=='.snappy.parquet', dbutils.fs.ls(dir.path)) )

i = i - 1  # exclude the transaction log folder

print('On the delta table, there are {0} files across {1} partition directories for an average of {2} files per partition'.format(n, i, float(n)/float(i)))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql -- idle time calculations
# MAGIC 
# MAGIC SELECT
# MAGIC   x.bikeid,
# MAGIC   y.end_station_id as station_id,
# MAGIC   y.end_station_name as station_name,
# MAGIC   y.end_station_latitude as station_latitude,
# MAGIC   y.end_station_longitude as station_longitude,
# MAGIC   x.stoptime as idle_starttime,
# MAGIC   x.next_starttime as idle_stoptime,
# MAGIC   unix_timestamp(x.next_starttime)-unix_timestamp(x.stoptime) as idle_seconds
# MAGIC FROM ( -- get next starttime for a given bike
# MAGIC   SELECT 
# MAGIC     a.stoptime,
# MAGIC     a.bikeid,
# MAGIC     min(b.starttime) as next_starttime
# MAGIC   FROM citibikenyc.trips_delta a
# MAGIC   JOIN citibikenyc.trips_delta b
# MAGIC     ON a.bikeid=b.bikeid AND a.stoptime < b.starttime
# MAGIC   GROUP BY 
# MAGIC     a.stoptime,
# MAGIC     a.bikeid
# MAGIC   ) x
# MAGIC JOIN citibikenyc.trips_delta y
# MAGIC   ON x.bikeid=y.bikeid AND x.stoptime=y.stoptime
# MAGIC ORDER BY idle_starttime ASC