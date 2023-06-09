from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from os.path import join as pjoin
import logging

def display_mobility_data(
        hdfs_host,
        hdfs_port = '9000',
        data_dir = 'apps/spark',
        dwh = 'mobilitydwh',
        driver_mem_gb=5, 
        exec_mem_gb=5, 
        **kwargs
    ):
    
    MEM_DRIVER_GB = driver_mem_gb
    MEM_EXEC_GB = exec_mem_gb
    
    spark = SparkSession.builder\
        .appName("UP_ML_scoring")\
        .config("spark.executor.memory", f"{exec_mem_gb}g")\
        .config("spark.executor.cores", 5)\
        .config("spark.driver.memory", f"{driver_mem_gb}g")\
        .config("spark.driver.maxResultSize", f"{driver_mem_gb}g")\
        .config("spark.local.dir", "/var/opt/anritsu/.pysparkTmp")\
        .getOrCreate()
    
    result_tb = spark.read.parquet(pjoin(f"hdfs://{hdfs_host}:{hdfs_port}", data_dir, dwh,'fact_tb','*')).limit(10)
    result_df = result_tb.toPandas()
    logging.info(result_df)
    logging.info('Loading successful')