# Databricks notebook source
# MAGIC %md The trip data was loaded on an hourly basis.  We didn't in fact load the data one-hour at a time over a 13 month period but it was loaded in separate waves that help one-hour's worth of data at a time.  While this is not easily detected in the parquet table, the delta table exposes this information through the DESCRIBE HISTORY statement:

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY citibikenyc.trips_delta;

# COMMAND ----------

# MAGIC %md This timestamp/version information is held by delta within the delta log.  The delta log is a folder named *_delta_log* placed under the tables primary folder.  In it is a series of JSON files that maintain statistics on the various parquet files that delta writes to the table, including the timestamp associated with each file's creation.
# MAGIC 
# MAGIC Why does this matter? The timestamp information is the basis of a mechanism implemented by delta to ensure transactional consistency on a table.  When a read operation is initiated against a delta table, the latest timestamp is retrieved and only files written up to and prior to that timestamp are avaialable to be consumed in that operation.  This allows multiple readers and writers to interact with the data while preserving snapshot isolation on the table.
# MAGIC 
# MAGIC A side-benefit of this is that we as users can perform time-travel on the data, i.e. we can see the data as it was at a point in time.  For example, we might wish to see average trip durations:
# MAGIC 
# MAGIC **NOTE**: The TIMESTAMP/VERSION AS OF statement is supported in Databricks 5.2 and above

# COMMAND ----------

# MAGIC %sql -- each hour of Jan 1, 2019
# MAGIC 
# MAGIC SELECT 
# MAGIC   hour(starttime) as hour,
# MAGIC   COUNT(*) as trips
# MAGIC FROM citibikenyc.trips_delta
# MAGIC WHERE startdate = '2019-01-01'
# MAGIC GROUP BY hour(starttime)
# MAGIC ORDER BY hour;

# COMMAND ----------

# MAGIC %sql -- each hour of Jan 1, 2019 as of mid-day load
# MAGIC 
# MAGIC SELECT 
# MAGIC   hour(starttime) as hour,
# MAGIC   COUNT(*) as trips
# MAGIC FROM citibikenyc.trips_delta
# MAGIC   VERSION AS OF 12	
# MAGIC WHERE startdate = '2019-01-01'
# MAGIC GROUP BY hour(starttime)
# MAGIC ORDER BY hour;

# COMMAND ----------

# MAGIC %md So why is this side-affect interesting?  With time-travel, we can have highly volatile data such as in an IOT/Streaming scenario but still present our users with a stable representation of the data (ideally through a view which hides the details).  This allows our users to get stable results from their queries within a well-understood window.
# MAGIC 
# MAGIC It also allows us as Data Engineers the ability to fall-back should we have a bad data load.  Consider this statement which duplicates our data on Jan 1, 2019 and sets all tripdurations on that date to 0.  This may be the result of a bad or duplicate ETL run:

# COMMAND ----------

# MAGIC %sql -- redundant ETL run duplicates data
# MAGIC 
# MAGIC INSERT INTO citibikenyc.trips_delta
# MAGIC SELECT *
# MAGIC FROM citibikenyc.trips_delta;
# MAGIC --WHERE startdate = '2019-01-01';
# MAGIC 
# MAGIC UPDATE citibikenyc.trips_delta
# MAGIC SET tripduration = 0;
# MAGIC --WHERE startdate = '2019-01-01';
# MAGIC 
# MAGIC -- NOTE: If you limit startdate to Jan 1, 2019, the code will run fast but the fragmentation in the 03 workbook will be reduced. If you do enable the WHERE clause, be sure to enable it in the rollback queries below as well.

# COMMAND ----------

# MAGIC %md With bad data in place, notice how our hourly figures on Jan 1, 2019 jump up:

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT 
# MAGIC   hour(starttime) as hour,
# MAGIC   COUNT(*) as trips
# MAGIC FROM citibikenyc.trips_delta
# MAGIC WHERE startdate = '2019-01-01'
# MAGIC GROUP BY hour(starttime)
# MAGIC ORDER BY hour;

# COMMAND ----------

# MAGIC %md Once I've determined that we had a bad ETL cycle, I can not only see the data just prior to that run, I can restore the table to the last good point in time.  While there are several ways to do this, I'll just delete all the data for that day and reload it from the last known good version:

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY citibikenyc.trips_delta;

# COMMAND ----------

# MAGIC %sql  -- rollback to version 23
# MAGIC 
# MAGIC DELETE FROM citibikenyc.trips_delta; 
# MAGIC --WHERE startdate = '2019-01-01';
# MAGIC 
# MAGIC INSERT INTO citibikenyc.trips_delta 
# MAGIC SELECT * FROM citibikenyc.trips_delta VERSION AS OF 23;
# MAGIC --WHERE startdate = '2019-01-01';
# MAGIC 
# MAGIC -- NOTE: Enable/Disable the where clauses based on the "bad etl" logic used above

# COMMAND ----------

# MAGIC %sql  -- see data back in good state
# MAGIC 
# MAGIC SELECT 
# MAGIC   hour(starttime) as hour,
# MAGIC   COUNT(*) as trips
# MAGIC FROM citibikenyc.trips_delta
# MAGIC WHERE startdate = '2019-01-01'
# MAGIC GROUP BY hour(starttime)
# MAGIC ORDER BY hour;

# COMMAND ----------

