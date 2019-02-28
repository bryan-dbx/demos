# Databricks notebook source
# MAGIC %md Authentication in Azure Databricks is through Azure Active Directory. Valid AAD User accounts are given access to Databricks through the Admin Console | Users page.
# MAGIC 
# MAGIC Note: Support for Azure AD Groups always comes up when we start talking about permissioning.  Please see this internal entry which identifies this as in Private Preview (https://databricks.aha.io/features/DB-83)
# MAGIC 
# MAGIC Once added, you assign permissions at workspace, cluster & asset levels:</p>
# MAGIC 
# MAGIC 1. Workspace (cross-cluster)
# MAGIC 2. Cluster-Specific
# MAGIC 3. Asset-Specific

# COMMAND ----------

# MAGIC %md At the Workspace level, permisssions are identified as:
# MAGIC 
# MAGIC </p>
# MAGIC 1. Admin - has full administrative capabilities across all assets in the workspace; implies Cluster Creation & User permissions
# MAGIC 2. Allow Cluster Creation - has ability to define new clusters; implies User permissions
# MAGIC 3. User - has ability to access the environment but has no administrative abilities
# MAGIC 
# MAGIC Most users should be no more than Users within the workspace.  A few trusted individuals may be granted rights to define clusters.  And very few individuals should have full Admin rights.

# COMMAND ----------

# MAGIC %md At the Cluster level, user permissions are assigned in the cluster configuration. Under the Advanced Options, choose the Permissions tab and then add Users (and Groups) to the cluster. Permissions which can then be assigned are:
# MAGIC 
# MAGIC ref: https://docs.databricks.com/administration-guide/admin-settings/cluster-acl.html#individual-cluster-permissions
# MAGIC </p>
# MAGIC 1. Can Manage - Full permissions to the cluster; implies lower-level permission
# MAGIC 2. Can Restart - Permissions ot start, restart and terminate clusters; implies lower-level permissions
# MAGIC 3. Can Attach To - Permissions to simply attache to a cluster and view the Spark UI/cluster metrics
# MAGIC </p>
# MAGIC 
# MAGIC NOTE: Individuals with Allow Cluster Creation permissions have Can Manage permissions on any cluster they create

# COMMAND ----------

# MAGIC %md At the Asset level, permissions are assigned to notebooks, folders, libraries, etc. at the following levels:
# MAGIC </p>
# MAGIC 1. None - by default, users do not have access to assets that they have not created and to which a permission has otherwise been assigned
# MAGIC 1. Can Read - view and comment
# MAGIC 2. Can Run - Can Read + run and attach/detach 
# MAGIC 3. Can Edit - Can Run + edit materials
# MAGIC 4. Can Manage - Can Edit + create, delete, and change permissions
# MAGIC </p>
# MAGIC Each user has a "home" directory within the Workspace within which they have Can Manage rights.
# MAGIC </p>
# MAGIC Please note that a special folder named Shared (at the root of the Workspace) defaults to Can Manage for all users.

# COMMAND ----------

