from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import delta as d
import delta.tables as dt

from os.path import join as pjoin

def merge_new_data(
        hdfs_host,
        hdfs_port = '9000',
        data_dir = 'apps/spark',
        dwh = 'mobilitydwh',
        driver_mem_gb=15, 
        exec_mem_gb=15, 
        id_col='HASHED_IMSI',
        **kwargs
    ):
    '''
    This is an anti-pattern. This function performs and expensive computation that should be
    submitted to a spark cluster or outsourced to the data warehouse.
    
    However, the cluster where I'm launching this pipeline has several constraints, and given
    the relatively small quantity of data I'm handling it's easier to do this.
    '''
    
    spark = SparkSession.builder\
        .appName("merge_mobility_data")\
        .config("spark.executor.memory", f"{exec_mem_gb}g")\
        .config("spark.executor.cores", 20)\
        .config("spark.driver.memory", f"{driver_mem_gb}g")\
        .config("spark.driver.maxResultSize", f"{driver_mem_gb}g")\
        .config("spark.local.dir", "/var/opt/anritsu/.pysparkTmp")\
        .getOrCreate()

    historical_data = spark.read.parquet(pjoin(f"hdfs://{hdfs_host}:{hdfs_port}", data_dir, dwh,'fact_tb','*'))
    new_data = spark.read.parquet(pjoin(f"hdfs://{hdfs_host}:{hdfs_port}", data_dir, dwh,'fact_tb_staging','*'))

    merged_data = historical_data\
        .union(new_data)\
        .drop_duplicates()
    
    merged_data.write\
        .mode('overwrite')\
        .partitionBy(id_col)\
        .parquet(pjoin(f"hdfs://{hdfs_host}:{hdfs_port}", data_dir, dwh,'fact_tb_updated')) 
    
def update_deltalake(
        hdfs_host,
        hdfs_port = '9000',
        data_dir = 'apps/spark',
        dwh = 'mobilitydwh',
        driver_mem_gb=15, 
        exec_mem_gb=15, 
        id_col='HASHED_IMSI',
        **kwargs
    ):
    '''
    This is an anti-pattern. This function performs and expensive computation that should be
    submitted to a spark cluster or outsourced to the data warehouse.
    
    However, the cluster where I'm launching this pipeline has several constraints, and given
    the relatively small quantity of data I'm handling it's easier to do this.
    '''

    spark_builder = SparkSession.builder\
        .appName("merge_mobility_data")\
        .config("spark.executor.memory", f"{exec_mem_gb}g")\
        .config("spark.executor.cores", 20)\
        .config("spark.driver.memory", f"{driver_mem_gb}g")\
        .config("spark.driver.maxResultSize", f"{driver_mem_gb}g")\
        .config("spark.local.dir", "/var/opt/anritsu/.pysparkTmp")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = d.configure_spark_with_delta_pip(spark_builder).getOrCreate()
    
    newData = spark.read.parquet(pjoin(f"hdfs://{hdfs_host}:{hdfs_port}", data_dir, dwh,'fact_tb_staging','*'))
    
    try: #Bad practice, I should check the existance of the hdfs path
        deltaTable = dt.DeltaTable.forPath(spark, pjoin(f"hdfs://{hdfs_host}:{hdfs_port}", data_dir, dwh,'fact_tb','*'))

        deltaTable.alias("oldData") \
            .merge(
                newData.alias("newData"),
                "oldData.USER_ID = newData.USER_ID and oldData.START_TIME_LOCAL = newData.START_TIME_LOCAL" ## check this
            )\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    except:
        newData.write.format("delta").save(pjoin(f"hdfs://{hdfs_host}:{hdfs_port}", data_dir, dwh,'fact_tb','*'))

        #        .whenMatchedUpdate(set = { "id": col("newData.id") }) \
        #    .whenNotMatchedInsert(values = { "id": col("newData.id") }) \
