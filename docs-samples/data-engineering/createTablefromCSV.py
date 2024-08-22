import sys
import os
#import Constant
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


if __name__ == "__main__":

    #Spark session builder
    spark_session = (SparkSession
          .builder
          .appName("sjdsampleapp") 
          .config("spark.some.config.option", "some-value")
          .getOrCreate())
    
    spark_context = spark_session.sparkContext
    spark_context.setLogLevel("DEBUG")
    


    print("spark.synapse.pool.name : " + spark_session.conf.get("spark.synapse.pool.name")) 
    print() 
    print("spark.driver.cores : " + spark_session.conf.get("spark.driver.cores")) 
    print("spark.driver.memory : " + spark_session.conf.get("spark.driver.memory")) 
    print("spark.executor.cores : " + spark_session.conf.get("spark.executor.cores")) 
    print("spark.executor.memory : " + spark_session.conf.get("spark.executor.memory")) 
    print("spark.executor.instances: " + spark_session.conf.get("spark.executor.instances")) 
    print() 
    print("spark.dynamicAllocation.enabled : " + spark_session.conf.get("spark.dynamicAllocation.enabled")) 
    print("spark.dynamicAllocation.maxExecutors : " + spark_session.conf.get("spark.dynamicAllocation.maxExecutors")) 
    print("spark.dynamicAllocation.minExecutors : " + spark_session.conf.get("spark.dynamicAllocation.minExecutors")) 
    
    #tableName = "yellowtripdata"
    # You can download the sample Parquet file from this site "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page" and upload it to the files section of the lakehouse. 
    parquetFilePath = "Files/yellow_tripdata_2022-01.parquet"
    #deltaTablePath = SaveToLH + "/Tables/" + tableName
    deltaTablePath = "Tables/yellowtrip"

    df = spark_session.read.format('parquet').load(parquetFilePath)
    df.write.mode('overwrite').format('delta').save(deltaTablePath)
