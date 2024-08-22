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



    print("spark.synapse.pool.name : " + spark.conf.get("spark.synapse.pool.name")) 
    print() 
    print("spark.driver.cores : " + spark.conf.get("spark.driver.cores")) 
    print("spark.driver.memory : " + spark.conf.get("spark.driver.memory")) 
    print("spark.executor.cores : " + spark.conf.get("spark.executor.cores")) 
    print("spark.executor.memory : " + spark.conf.get("spark.executor.memory")) 
    print("spark.executor.instances: " + spark.conf.get("spark.executor.instances")) 
    print() 
    print("spark.dynamicAllocation.enabled : " + spark.conf.get("spark.dynamicAllocation.enabled")) 
    print("spark.dynamicAllocation.maxExecutors : " + spark.conf.get("spark.dynamicAllocation.maxExecutors")) 
    print("spark.dynamicAllocation.minExecutors : " + spark.conf.get("spark.dynamicAllocation.minExecutors")) 
    
    #tableName = "yellowtripdata"
    # You can download the sample CSV file from this site "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page" and upload it to the files section of the lakehouse. 
    csvFilePath = "Files/yellow_tripdata_2022_01.csv"
    #deltaTablePath = SaveToLH + "/Tables/" + tableName
    deltaTablePath = "Tables/yellowtrip"

    df = spark_session.read.format('csv').options(header='true', inferschema='true').load(csvFilePath)
    df.write.mode('overwrite').format('delta').save(deltaTablePath)
