# Databricks notebook source
# MAGIC %md In this notebook, I will provide you the instructions for setting up an Azure AD (AAD) environment with which you can demonstrate the various authentication/authorization mechanisms within Azure Databricks.  This assumes you have access and appropriate rights to an Azure Subscription within which you have already deployed your Azure Databricks workspace.
# MAGIC 
# MAGIC #####NOTE: The instructions here are quite laboreous.  You might just want to ask Bryan for use of the AAD Directory & Users he's already created.

# COMMAND ----------

# MAGIC %md ###Setup a New Azure AD Directory
# MAGIC </p>
# MAGIC 1. Login to the Azure Portal
# MAGIC 2. Navigate to the AAD item
# MAGIC 3. Create a new AAD Directory (link to this is currently in far, bottom right-hand corner of AAD Overview page under Your Role)
# MAGIC   * Organization name should be unique and identify you, *e.g.* brysmi-dbx
# MAGIC   * Initial domain should be unique and appropriate for customer demos, *e.g.* brysmidbx
# MAGIC 4. Switch Directory to your newly created directory

# COMMAND ----------

# MAGIC %md ###Create User Accounts
# MAGIC </p>
# MAGIC Unfortunately, there is no way to automate this via PowerShell or CLI today.  From the new AAD Directory, manually setup users by:
# MAGIC </p>
# MAGIC 1. Under Manage, select Users
# MAGIC 2. Select New User
# MAGIC 3. Provide Name and User Name
# MAGIC 4. Leave all other properties at defaults
# MAGIC 5. Move your cursor to trigger Password box to appear
# MAGIC 6. Select Show Password
# MAGIC 5. Copy the Password when exposed and save for later use
# MAGIC 6. Click Create
# MAGIC 7. Repeat until all required users in place
# MAGIC </p>
# MAGIC 
# MAGIC With user accounts in place, you now need to set passwords to standard passwords:
# MAGIC 
# MAGIC </p>
# MAGIC 
# MAGIC 1. Open a browser in InPrivate/InCognito mode
# MAGIC 2. Login the the Azure Portal using one of the newly created usernames and passwords
# MAGIC 3. Follow the prompts to create a new password
# MAGIC 4. Repeat for all user accounts
# MAGIC 
# MAGIC </p>
# MAGIC 
# MAGIC With passwords set, you now need to add these users to your Subscriptions's AAD directory as guest users:
# MAGIC </p>
# MAGIC 
# MAGIC 1. Return to AAD within the Azure Portal
# MAGIC 2. Switch back to the AAD directory that holds your Databricks workspace
# MAGIC 3. Select Users
# MAGIC 4. Click Add Guest User
# MAGIC 5. Add the User ID and take defaults on all other options.
# MAGIC 
# MAGIC **NOTE**: The first time an account is used, the user will be prompted to "Review Permissions". Instruct them to acept the prompt or do this for them in advance of any demo.

# COMMAND ----------

# MAGIC %md ###Add Users to Your Databricks Workspace
# MAGIC </p>
# MAGIC With user accounts created, now you can add users to your workspace.  To do this, simply:
# MAGIC </p>
# MAGIC 1. Click on the User Icon in the upper right-hand corner of the Databricks environment
# MAGIC 2. Select Admin Console
# MAGIC 3. On the Users tab, add your users to your system with appropriate roles

# COMMAND ----------

