# Databricks notebook source
# MAGIC %md #### SETUP
# MAGIC 
# MAGIC Before running this demonstration, you need to deploy the following to the region within which you run Azure Databricks:
# MAGIC </p>
# MAGIC * HDInsight Hive/LLAP Cluster, *e.g.* brysmihdi, configured with external metastore, *e.g.* brysmimeta/metastore
# MAGIC * Azure Storage Account, *e.g.* brysmiwasb
# MAGIC </p>
# MAGIC NOTE The official documentation for this scenario is found here: https://docs.azuredatabricks.net/user-guide/advanced/external-hive-metastore.html

# COMMAND ----------

# MAGIC %md In this demonstration, we have an HDInsight cluster with an external metastore. We deploy our cluster with the following configuration settings.  These can be assigned in the cluster config or alternatively through an init script:
# MAGIC </p>
# MAGIC * spark.sql.hive.metastore.version 1.2.1
# MAGIC * spark.sql.hive.metastore.jars builtin
# MAGIC * javax.jdo.option.ConnectionURL *mssql-connection-string*
# MAGIC * javax.jdo.option.ConnectionUserName *mssql-username*
# MAGIC * javax.jdo.option.ConnectionPassword *mssql-password*
# MAGIC * javax.jdo.option.ConnectionDriverName com.microsoft.sqlserver.jdbc.SQLServerDriver
# MAGIC * hive.metastore.schema.verification.record.version true
# MAGIC * hive.metastore.schema.verification **false**
# MAGIC 
# MAGIC Notice that I'm setting the schema verification setting to false.  What I'm doing here is telling Spark that I don't want it to confirm that the version of the metastore matches its expectations. There is a risk in doing this that some information in the metastore isn't compatible with what Spark expects but I'd rather manage compatibility externally instead of getting an error when there is a slight version difference.
# MAGIC 
# MAGIC How do we know the spark.sql.hive.metastore.version?  The best way I've found to do this to:
# MAGIC 
# MAGIC 1. Launch Ambari
# MAGIC 2. Select Admin | Stacks and Versions
# MAGIC 3. Locate Hive in the Drop-Down List
# MAGIC 
# MAGIC With this set, you should now be able to see the table definitions in the metastore:

# COMMAND ----------

# MAGIC %sql show tables;

# COMMAND ----------

# MAGIC %md Notice that you cannot yet access data from these tables. So while you've configured access to their metadata, you don't yet have access to the storage location:

# COMMAND ----------

# MAGIC %sql select * from hivesampletable;

# COMMAND ----------

# MAGIC %md To understand why this is, take a look at the definition for this table and note the location information:

# COMMAND ----------

# MAGIC %sql describe formatted hivesampletable;

# COMMAND ----------

# MAGIC %md Each Hive table points to a storage location.  The Databricks cluster can read the metastore but doesn't yet have access to the storage account. By configuring credentials for the storage account, we overcome this problem.  While we are demonstrating this in code, this can be done with the cluster configuration.  If done in code, it should be done using the Secrets capability:

# COMMAND ----------

spark.conf.set(
  'fs.azure.account.key.brysmiwasb.blob.core.windows.net',
  'o9DJeWaza76HPB4neJe+ptYAEa6boFH2KjzAsrL2vLWy/UkR2CP7cvf+CPYHY3mezE6EC2rC3/e0zWTvlG7jpA=='
  )

# COMMAND ----------

# MAGIC %sql select * from hivesampletable;

# COMMAND ----------

# MAGIC %md So, what do we do when there is a version compatibility issue that doesn't allow a shared metastore?  Our best bet is to then script our tables using SHOW CREATE TABLE and then run this on our Spark cluster:
# MAGIC 
# MAGIC NOTICE that this method has no dependency on the cluster configs addressed at the top of this notebook.

# COMMAND ----------

import jaydebeapi as jdbc

jdbc_conn_string = 'jdbc:hive2://brysmihdi.azurehdinsight.net:443/default;transportMode=http;ssl=true;httpPath=/hive2;user=admin;password=Aug082006!!!;'

conn = jdbc.connect( 
    'org.apache.hive.jdbc.HiveDriver', 
    jdbc_conn_string 
  )
curs = conn.cursor()
curs.execute('show create table default.hivesampletable')

meta = curs.fetchall()  #(' '.join(w) for w in sixgrams)
conn.close()



# COMMAND ----------

sql = ''
for m in meta:
  sql += m[0]+'\n'
  
sql = sql[:-1]
print(sql)

# COMMAND ----------

