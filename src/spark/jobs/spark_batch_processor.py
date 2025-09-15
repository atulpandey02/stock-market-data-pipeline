import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    spark = (SparkSession.builder
        .appName("StockMarketBatchProcessor")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate())
    
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    return spark

def main():
    logger.info("Starting batch processing...")
    spark = create_spark_session()
    
    try:
        # Define schema
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_percent", StringType(), True),
            StructField("volume", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        # Read data from raw path
        today = datetime.now()
        input_path = f"s3a://stock-market-data/raw/realtime/year={today.year}/month={today.month:02d}/day={today.day:02d}/"
        output_path = f"s3a://stock-market-data/processed/realtime/year={today.year}/month={today.month:02d}/day={today.day:02d}/"
        
        logger.info(f"Reading from: {input_path}")
        logger.info(f"Writing to: {output_path}")
        
        # Read CSV data
        df = spark.read.schema(schema).option("header", "true").csv(input_path)
        
        if df.count() == 0:
            logger.warning("No data found to process")
            return
            
        # Process data - add simple aggregations
        processed_df = (df
            .withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("price", F.col("price").cast(DoubleType()))
            .withColumn("change", F.col("change").cast(DoubleType()))
            .withColumn("processing_time", F.current_timestamp())
            .withColumn("window_start", F.date_trunc("minute", F.col("timestamp")))
            .withColumn("window_15m_end", F.date_add(F.col("window_start"), 15))
        )
        
        logger.info(f"Processing {processed_df.count()} records")
        
        # Write partitioned by symbol as parquet
        (processed_df
            .coalesce(1)  # Single file per partition
            .write
            .mode("overwrite")  # Overwrite for this batch
            .partitionBy("symbol")
            .parquet(output_path))
        
        logger.info("Batch processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
