#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark Batch Processor for Real-Time Stock Data

MINIMAL CHANGES from original streaming processor - just converted to batch mode.
Same transformation logic, same output structure, same column names.
"""

import logging
import os
import sys
import traceback
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configure logging - SAME AS ORIGINAL
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# S3/MinIO configuration - SAME AS ORIGINAL
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "stock-market-data"
MINIO_ENDPOINT = "http://minio:9000"


def create_spark_session():
    """Create and configure a Spark session - SAME CONFIG AS ORIGINAL."""
    logger.info("Initializing Spark session with S3 configuration for batch processing...")
    
    spark = (SparkSession.builder
        .appName("StockMarketBatchProcessor")  # Only changed name
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate())
    
    # Force shuffle partitions setting - SAME AS ORIGINAL
    spark.conf.set("spark.sql.shuffle.partitions", 2)
    
    # Configure S3A filesystem - SAME AS ORIGINAL
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    # Set Spark log level - SAME AS ORIGINAL
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized successfully!")
    
    return spark

def define_schema():
    """Define schema for the stock data - EXACT SAME AS ORIGINAL."""
    logger.info("Defining schema for stock data...")
    return StructType([
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), True),
        StructField("change", DoubleType(), True),
        StructField("change_percent", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

def read_batch_from_s3(spark):
    """
    Read batch data from S3/MinIO - CHANGED FROM readStream to read
    """
    logger.info("\n--- Setting Up Batch Read from S3 ---")
    
    # Define the schema - SAME AS ORIGINAL
    schema = define_schema()
    
    # Path where real-time data is stored - SAME AS ORIGINAL
    today = datetime.now()
    s3_path = f"s3a://{MINIO_BUCKET}/raw/realtime/year={today.year}/month={today.month:02d}/day={today.day:02d}/"
    logger.info(f"Reading batch data from: {s3_path}")
    
    try:
        # Read batch data from S3 - ONLY CHANGE: read instead of readStream
        batch_df = (spark.read
            .schema(schema)
            .option("header", "true")
            .csv(s3_path))
        
        if batch_df.count() == 0:
            logger.warning("No data found for processing")
            return None
        
        # Clean and transform the data - EXACT SAME AS ORIGINAL
        streaming_df = (batch_df
            .withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("price", F.col("price").cast(DoubleType()))
            .withColumn("change", F.col("change").cast(DoubleType()))
            .withColumn("change_percent", F.regexp_replace("change_percent", "%", ""))
            .withColumn("change_percent", F.col("change_percent").cast(DoubleType()))
            .withColumn("volume", F.col("volume").cast(IntegerType())))
        
        logger.info("Batch DataFrame schema:")
        logger.info("\n" + streaming_df._jdf.schema().treeString())
        
        return streaming_df
    except Exception as e:
        logger.error(f"Error setting up batch read from {s3_path}: {e}")
        logger.error(traceback.format_exc())
        return None

def process_streaming_data(streaming_df):
    """
    Process stock data to calculate real-time metrics - EXACT SAME AS ORIGINAL
    """
    logger.info("\n--- Processing Stock Data ---")
    
    if streaming_df is None:
        logger.info("No streaming data to process")
        return None
    
    try:
        # Add watermark to handle late data - SAME AS ORIGINAL
        streaming_df = streaming_df.withWatermark("timestamp", "5 minutes")
        
        # Define sliding windows for real-time metrics - EXACT SAME AS ORIGINAL
        window_15min = F.window("timestamp", "15 minutes", "5 minutes")  # 15-minute window, sliding every 5 minutes
        window_1h = F.window("timestamp", "1 hour", "10 minutes")  # 1-hour window, sliding every 10 minutes
        
        # Step 1: Compute metrics for the 15-minute window - EXACT SAME AS ORIGINAL
        df_15min = (streaming_df
            .groupBy(
                F.col("symbol"),
                window_15min.alias("window")
            )
            .agg(
                F.avg("price").alias("ma_15m"),
                F.stddev("price").alias("volatility_15m"),
                F.sum("volume").alias("volume_sum_15m")
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window"))
        
        # Step 2: Compute metrics for the 1-hour window - EXACT SAME AS ORIGINAL
        df_1h = (streaming_df
            .groupBy(
                F.col("symbol"),
                window_1h.alias("window")
            )
            .agg(
                F.avg("price").alias("ma_1h"),
                F.stddev("price").alias("volatility_1h"),
                F.sum("volume").alias("volume_sum_1h")
            )
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window"))
        
        # Step 3: Join the two DataFrames on symbol and window_start - EXACT SAME AS ORIGINAL
        processed_df = (df_15min
            .join(
                df_1h,
                (df_15min.symbol == df_1h.symbol) & 
                (df_15min.window_start == df_1h.window_start),
                "inner"
            )
            .select(
                df_15min.symbol,
                df_15min.window_start.alias("window_start"),
                df_15min.window_end.alias("window_15m_end"),
                df_1h.window_end.alias("window_1h_end"),
                df_15min.ma_15m,
                df_1h.ma_1h,
                df_15min.volatility_15m,
                df_1h.volatility_1h,
                df_15min.volume_sum_15m,
                df_1h.volume_sum_1h
            ))
        
        logger.info("Processed DataFrame schema:")
        logger.info("\n" + processed_df._jdf.schema().treeString())
        
        return processed_df
    except Exception as e:
        logger.error(f"Error processing stock data: {e}")
        logger.error(traceback.format_exc())
        return None

def write_batch_to_s3(processed_df):
    """Write processed data to S3 - CHANGED FROM writeStream to write"""
    logger.info("\n----- Writing Processed Data to S3")

    if processed_df is None:
        logger.error("No processed DataFrame to write to S3")
        return None 
    
    today = datetime.now()
    output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/year={today.year}/month={today.month:02d}/day={today.day:02d}/"
    logger.info(f"Writing processed data to: {output_path}")

    try:
        # Write batch data - ONLY CHANGE: write instead of writeStream
        processed_df.write \
            .mode("overwrite") \
            .partitionBy("symbol") \
            .parquet(output_path)
        
        logger.info(f"Batch processing completed, written to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error writing data to S3: {e}")
        logger.error(traceback.format_exc())
        return None

def main():
    """Main function - MINIMAL CHANGES from original."""
    logger.info("\n=========================================")
    logger.info("STARTING STOCK MARKET BATCH PROCESSOR")
    logger.info("=========================================\n")
    
    # Create Spark session - SAME AS ORIGINAL
    spark = create_spark_session()
    
    try:
        # Read batch data from S3 - CHANGED FROM read_stream_from_s3
        streaming_df = read_batch_from_s3(spark)
        
        if streaming_df is not None:
            # Process data - EXACT SAME FUNCTION AS ORIGINAL
            processed_df = process_streaming_data(streaming_df)
            
            if processed_df is not None:
                # Write processed data to S3 - CHANGED FROM write_stream_to_s3
                result = write_batch_to_s3(processed_df)
                
                if result:
                    logger.info("\nBatch processing completed successfully!")
                else:
                    logger.info("\nFailed to write processed data")
            else:
                logger.info("\nNo processed data to write")
        else:
            logger.info("\nNo data to process")
            
    except Exception as e:
        logger.error(f"\nError in batch processing: {e}")
        logger.error(traceback.format_exc())
    finally:
        # Stop Spark session - SAME AS ORIGINAL
        logger.info("\nStopping Spark session")
        spark.stop()
        logger.info("\n=========================================")
        logger.info("BATCH PROCESSING COMPLETED")
        logger.info("=========================================")

if __name__ == "__main__":
    main()