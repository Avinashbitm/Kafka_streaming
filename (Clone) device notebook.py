# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

confluentBootstrapservers ='pkc-12576z.us-west2.gcp.confluent.cloud:9092'
confluentApiKey = 'CIN5RKEL6QQO7KEZ'
confluentSecret = 'FbucV9jy60atnZkb2sTKIz3MC+D+bGo/ys+3WAHtQDVdOzMyfUzJAFYIg5qFXtAO'
confluentTopicName = 'device'
confluentTargetTopicName = 'iot_device'

# COMMAND ----------

device_df = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers",confluentBootstrapservers) \
.option("kafka.security.protocol","SASL_SSL") \
.option("kafka.sasl.mechanism","PLAIN") \
.option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
.option("kafka.ssl.endpoint.identification.algorithm","https") \
.option("subscribe",confluentTopicName) \
.option("startingTimestamp",1) \
.option("maxOffsetsPerTrigger",50) \
.load()

# COMMAND ----------

device_df.printSchema()

# COMMAND ----------

converted_device = device_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")

# COMMAND ----------

display(converted_device)

# COMMAND ----------

device_schema = "deviceId String,name1 String ,createdAt String"

# COMMAND ----------


parsed_device_df = converted_device.select("key",from_json("value",device_schema).alias("value"),"topic","partition","offset","timestamp","timestampType")

# COMMAND ----------

display(parsed_device_df)

# COMMAND ----------

parsed_device_df.createOrReplaceTempView("parsed_device_df")

# COMMAND ----------

from pyspark.sql.functions import explode, array

# Convert the struct to an array of structs
array_df = parsed_device_df.select("key", array("value").alias("value_array"))

# Explode the array into individual rows
exploded_df = array_df.select("key", explode(col("value_array")).alias("device"))

# Select the required columns
result_df = exploded_df.select("device.deviceId", "device.name1", "device.createdAt")

display(result_df)

# COMMAND ----------

result_df.createOrReplaceTempView('result_df')

# COMMAND ----------

# Clean the checkpoint directory
dbutils.fs.rm("/mnt/device-sync/checkpoint1/", recurse=True)



# COMMAND ----------

# Check if the mount point exists before unmounting
mount_point = "/mnt/deviceoutput/device1/"
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

# Mount the storage account
dbutils.fs.mount(
    source='wasbs://deviceoutput@ttstorageaccount999.blob.core.windows.net',
    mount_point="/mnt/deviceoutput/device1/",
    extra_configs={
        'fs.azure.account.key.ttstorageaccount999.blob.core.windows.net': 
        'g1YSa5v8tNG+mgsEp0HyhDOXYqSYV04AER33o+LIYH1EnJlppOAOq+lYn4FviZO1ultO51+ULSfb+AStiPbBFw=='
    }
)

# COMMAND ----------

# Repartition the DataFrame to a single partition before writing
result_df = result_df.repartition(1)

# Write the DataFrame to CSV in streaming mode
result_df.writeStream \
    .format("csv") \
    .option("path", "/mnt/deviceoutput/device1/processed") \
    .option("checkpointLocation", "/mnt/deviceoutput/device1") \
    .start()

# COMMAND ----------

# MAGIC %fs ls /mnt/deviceoutput/device1/processed/

# COMMAND ----------


