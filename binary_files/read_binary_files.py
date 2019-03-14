# Databricks notebook source
# MAGIC %md To demonstrate the reading of data from a large binary file in Azure Storage, you will need to [create an Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-quickstart-create-account?tabs=azure-portal) and [setup a container in that account](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-portal#create-a-container). In this account, we will later create a *large* binary file with which we can test our code. The name of the Azure Storage Account, the container, [a key](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-manage#view-and-copy-access-keys) with which you can access these resources and the name of the test file can be pasted in the cell below:
# MAGIC 
# MAGIC **NOTE** You should never paste a key in clear text in a production scenario. Instead, make use of the [Databricks Secrets](https://docs.azuredatabricks.net/user-guide/secrets/index.html) capability to protect such information.

# COMMAND ----------

storage_account_name = 'brysmitest'
storage_account_container_name = 'test'
storage_account_key = 'llUyOZDXEXF4v1OvMhvIDPuvAoRHfIdOk9751hNKImFeKHqO+Trter+qvWPFrfRQMIze83mAQpSNsaCiR1Xktg=='

test_blob_name = 'data.dat'

# COMMAND ----------

# MAGIC %md Now we need to create the binary test file with which we can test our code. To do this, we will first need to load the azure-storage-blob library (within the context of just this notebook):

# COMMAND ----------

if not filter(lambda lib: lib.startswith('PyPI:azure-storage-blob-') , dbutils.library.list()):
  dbutils.library.installPyPI('azure-storage-blob')
  
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import BlobBlock

# COMMAND ----------

# MAGIC %md With the library in place, we can now create the test file:

# COMMAND ----------

import os

# define target number of GB for test file
test_blob_target_size_GB = 10  # integer value required, no greater than 3125 due to use of 64 MB blocks below

# setup connection to azure storage
block_blob_service = BlockBlobService( storage_account_name, account_key=storage_account_key)

# write blocks for blob
block_list = []

for GB in range(0, test_blob_target_size_GB):      # for each targetted GB 
  for block in range(0, 16):                       #    write 16 x 64MB blocks 
    
    # generate 64 MB block of random bytes
    content = os.urandom(67108864)
    
    # create an id for this block
    block_id = 'block-'+str(GB).rjust(3,'0')+'-'+str(block).rjust(9,'0')
    
    # write the block
    block_blob_service.put_block(
      storage_account_container_name, 
      test_blob_name, 
      block_id = block_id, 
      block = content
      )
    
    # append block to list of blocks for this blob
    block_list.append( BlobBlock( id = block_id ) )

# commit blocks to blob
block_blob_service.put_block_list( storage_account_container_name, test_blob_name, block_list)

# COMMAND ----------

block_blob_service = BlockBlobService( storage_account_name, account_key=storage_account_key)

# determine the size of the blob
blob_size_bytes = block_blob_service.get_blob_properties( storage_account_container_name, test_blob_name ).properties.content_length

# determine the size of the test ranges
number_of_test_ranges = 128
bytes_in_range = blob_size_bytes / number_of_test_ranges

# initialize range variables
ranges = []
start_offset = 0

# for each range 
for r in range(0, number_of_test_ranges):
  
  # calculate end of range relative to last start
  stop_offset = start_offset + bytes_in_range
  
  # capture range start & stop
  ranges.append( (start_offset, stop_offset) )
  
  # increment the start
  start_offset = stop_offset + 1

# print first five ranges in our list so you can see what we've assembled
print(ranges[:5])

# COMMAND ----------

# MAGIC %md With the ranges defined, let's write a function to receive information about the targetted blob and the range of bytes to access and return those bytes:

# COMMAND ----------

import io

def read_blob_bytes( bbs, container_name, blob_name, start_offset, stop_offset):
  
  # define a stream object to receive bytes from blob
  blob_stream = io.BytesIO()
  
  # request bytes from blob
  bbs.get_blob_to_stream(
    container_name, 
    blob_name, 
    stream=blob_stream, 
    start_range=start_offset, 
    end_range=stop_offset
    )
  
  # return those bytes along with some info about the targetted blob
  return (container_name, blob_name, start_offset, stop_offset , blob_stream.getvalue())

# COMMAND ----------

# MAGIC %md Now we can execute our reads into an RDD:

# COMMAND ----------

# parallelize range definitions within an RDD
ranges_rdd = sc.parallelize( ranges, numSlices=sc.defaultParallelism )

# for each range definition, read bytes from blob
bytes_rdd = ranges_rdd.map(lambda rng: read_blob_bytes(
                            block_blob_service, 
                            storage_account_container_name, 
                            test_blob_name, 
                            rng[0], 
                            rng[1]
                            )
                          ).cache()

# COMMAND ----------

# MAGIC %md Let's now see how many elements we have in our RDD.  This should align with the number of ranges defined in earlier cells:

# COMMAND ----------

# count rdd rows
bytes_rdd.count()

# COMMAND ----------

# MAGIC %md And let's also take a look at the data associated with one the RDD elements:

# COMMAND ----------

# present structure of rdd
bytes_rdd.take(1)