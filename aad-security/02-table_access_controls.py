# Databricks notebook source
# MAGIC %md By default, users have access to all data associated with a clsuter. Data stores mounted to DBFS are accessible across all clusters.  Cluster configurations can be modified so that specific stores are only accessible from that cluster.  Combining this with cluster access permissions, you can limit data availablity to some degree, the trade-off being that users of that clsuter must access data using long-form notation, e.g. wasbs://container@storageaccount.blob.core.windows.net/blob.

# COMMAND ----------

# MAGIC %md For finer-grain access controls, Table Access Controls (TAC) can be enabled (req's Databricks Premium SKU) within a workspace.  Clusters configured to use these controls then prevent users from accessing the underlying storage, instead forcing access through tables and then applying object-level permission on the tables.
# MAGIC </p>
# MAGIC To enable TAC within a workspace:</p>
# MAGIC 1. Admin Console
# MAGIC 2. Access Control
# MAGIC 3. Enable Table Access Controls
# MAGIC </p>
# MAGIC With TAC enabled, you can now apply the feature on a per-cluster basis.

# COMMAND ----------

# MAGIC %md When createing a TAC enabled cluster, you must go to the Advanced Options and select the Enable Table Access Control and Only Allow Python & SQL Commands option.  What you will notice is that selecting this option adds two options to the Spark Config:
# MAGIC </p>
# MAGIC * spark.databricks.repl.allowedLanguages python,sql
# MAGIC * spark.databricks.acl.dfAclsEnabled true
# MAGIC 
# MAGIC Now when users attach to this cluster, they can use Python and SQL to access data objects per permissions assigned to the cluster and the objects in the workspace.

# COMMAND ----------

# MAGIC %md With TAC enabled, you can now assign users the rights to attach to the cluster.  Let's assume we've done this for all users.
# MAGIC 
# MAGIC Let's now create a database environment within which we can test TAC permissions:

# COMMAND ----------

# this script needs to be run from a non-TAC-enabled cluster
# because we are defining data assets to be persisted to storage

spark.sql('drop database if exists tacdbA cascade')
spark.sql('drop database if exists tacdbB cascade')

spark.sql('create database tacdbA')
spark.sql('create database tacdbB')

rdd = sc.parallelize(xrange(0,1000)).map(lambda x: (x, 0))
df = spark.createDataFrame( rdd, schema=['col1', 'col2'] ).cache()
df.write.saveAsTable('tacdbA.table01', format='parquet')
df.write.saveAsTable('tacdbA.table02', format='parquet')
df.write.saveAsTable('tacdbB.table03', format='parquet')
df.write.saveAsTable('tacdbB.table04', format='parquet')

spark.sql('''
create view tacdbA.table02_evens as
  select *
  from tacdbA.table02
  where col1 % 2 == 0
  '''
  )

# COMMAND ----------

# MAGIC %md Let's now assign permissions to these objects:

# COMMAND ----------

# MAGIC %sql -- THIS MUST BE RUN ON THE TACDB CLUSTER
# MAGIC 
# MAGIC -- grant nothing to `user01@brysmidbx.onmicrosoft.com`
# MAGIC 
# MAGIC GRANT SELECT ON DATABASE tacdbA TO `user02@brysmidbx.onmicrosoft.com`;
# MAGIC 
# MAGIC GRANT SELECT ON TABLE tacdbA.table02 TO `user03@brysmidbx.onmicrosoft.com`;
# MAGIC 
# MAGIC ALTER TABLE tacdbA.table02 OWNER TO `bryan.smith@databricks.com`;
# MAGIC 
# MAGIC GRANT SELECT ON VIEW tacdbA.table02_evens TO `user04@brysmidbx.onmicrosoft.com`;
# MAGIC ALTER VIEW tacdbA.table02_evens OWNER TO `bryan.smith@databricks.com`;
# MAGIC 
# MAGIC 
# MAGIC -- NOTE We are having to assign ownership to these objects because they were created on non-TAC-enabled cluster.
# MAGIC --      This is a side-effect of how we setup the dataframe and saved it.  if we had saved the data to file and
# MAGIC --      then issued a CREATE TABLE statement from the TAC-enabled cluster, ownership would have been implicitly
# MAGIC --      granted at that time.

# COMMAND ----------

# MAGIC %sql SHOW GRANT `user02@brysmidbx.onmicrosoft.com` ON DATABASE tacdbA

# COMMAND ----------

# MAGIC %md To demonstrate permissions, you can open a browser in InPrivate/Incognito mode, logging into Databricks as each of the users:
# MAGIC 
# MAGIC * user01@brysmidbx.onmicrosoft.com
# MAGIC * user02@brysmidbx.onmicrosoft.com
# MAGIC * user03@brysmidbx.onmicrosoft.com
# MAGIC * user04@brysmidbx.onmicrosoft.com

# COMMAND ----------

# MAGIC %md User01 should be able to attach to the tac-enabled cluster but doesn't have permissions to any objects.  This user should see no data.

# COMMAND ----------

# MAGIC %md User02 has full read access to objects in the tacdbA database.  This user should be able to see nothing in tacdbB.

# COMMAND ----------

# MAGIC %md User03 has access to table02 in tacdbA.  Table01 in this database should not be accessible and all objects in the tacdbB database are not accessible.

# COMMAND ----------

# MAGIC %md User04 has access to the table02_evens view in tacdbA.  This user cannot directly access the underlying tables and has no access to tacdbB.

# COMMAND ----------

# MAGIC %md In addition to GRANT permissions, you can apply DENY permissions to achieve a permissioning scheme aligned with your needs.  More info on permissioning hierachies is found here:https://docs.databricks.com/administration-guide/admin-settings/table-acls/object-permissions.html

# COMMAND ----------

