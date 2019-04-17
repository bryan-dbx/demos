# Databricks notebook source
# MAGIC %md As before, we need to load the Azure Storage Blob library into the notebook:

# COMMAND ----------

dbutils.library.installPyPI('azure-storage-blob')
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md To demonstrate the writing of data to a large binary file in Azure Storage, you will need to create an Azure Storage Account and setup a container in that account. The name of the Azure Storage Account, the container, a key with which you can access these resources and the name of the test file can be pasted in the cell below:
# MAGIC 
# MAGIC NOTE You should never paste a key in clear text in a production scenario. Instead, make use of the Databricks Secrets capability to protect such information.

# COMMAND ----------

storage_account_name = 'brysmiwasb'
storage_account_container_name = 'test'
storage_account_key = 'o9DJeWaza76HPB4neJe+ptYAEa6boFH2KjzAsrL2vLWy/UkR2CP7cvf+CPYHY3mezE6EC2rC3/e0zWTvlG7jpA=='

test_blob_name = 'data.out'
test_blob_target_size_GB = 10

# COMMAND ----------

# MAGIC %md With the account in place, we can now define the blocks that will make up our block blob file.  Let's use a default block size of 100 MB, the largest block size currently allowed by Azure Storage block blobs:

# COMMAND ----------

# define block size in bytes
block_bytes = 104857600

# convert target size from GB to bytes
test_blob_target_bytes = test_blob_target_size_GB * 1073741824

# calculate number of whole blocks in target file
whole_blocks = test_blob_target_bytes // block_bytes

# calculate remaining bytes in last block
remaining_bytes = test_blob_target_bytes % block_bytes

# build list of whole blocks required for file
blocks = list( zip( range(0, whole_blocks), [block_bytes] * whole_blocks ) )

# add remainder block if needed to complete file
if remaining_bytes != 0:
  blocks += [(whole_blocks, remaining_bytes)]

# COMMAND ----------

# MAGIC %md We can now define a function to write individual blocks to the blob:

# COMMAND ----------

from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import BlobBlock
import os

def write_block( bbs, container_name, blob_name, block_number, byte_count):
  
  # create an id for this block
  block_id = 'block-'+str(block_number).rjust(5,'0')
  
  # generate random bytes
  content = os.urandom(byte_count)
    
  # write the block
  block_blob_service.put_block(
    container_name, 
    blob_name, 
    block_id = block_id, 
    block = content
    )
    
  # return the block object
  return BlobBlock( id = block_id )

# COMMAND ----------

# MAGIC %md Using this function we can now write the blocks, assembling the list of newly written blocks:

# COMMAND ----------

# define connection info for storage account
block_blob_service = BlockBlobService( storage_account_name, account_key=storage_account_key)

# write blocks
blocks_rdd = sc.parallelize( blocks ).map(lambda b: write_block(block_blob_service, storage_account_container_name, test_blob_name, b[0], b[1]))

# retrieve block objects as sorted list
block_list = blocks_rdd.collect()
block_list.sort(reverse=False, key=lambda b: b.id)

# COMMAND ----------

# MAGIC %md At this point, the blob exists in an uncommitted state.  By committing the list of blocks that make up the blob, the blob should become available for broader use:

# COMMAND ----------

# commit blocks to blob using sorted block object list
block_blob_service.put_block_list( storage_account_container_name, test_blob_name, block_list)
