#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark Batch Analytics Processor for Stock Market Data

This script processes collected streaming data in batch mode, applying analytics
transformations like moving averages, volatility calculations, and time-window aggregations.
Designed to run reliably with timeouts and proper resource management.
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Configuration
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "stock-market-data"
MINIO_ENDPOINT = "http://minio:9000"


def create_spark_session():
    """Create and configure Spark session for batch processing."""
    logger.info("Initializing Spark session for batch analytics...")
    
    spark = (SparkSession.builder
        .appName("StockMarketBatchAnalytics")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate())
    
    # Configure S3A for MinIO
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized successfully")
    
    return spark


def define_schema():
    """Define schema for stock data."""
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("change", DoubleType(), True),
        StructField("change_percent", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])


def read_raw_data(spark):
    """Read raw streaming data collected earlier."""
    logger.info("Reading raw streaming data...")
    
    today = datetime.now()
    input_path = f"s3a://{MINIO_BUCKET}/raw/realtime/year={today.year}/month={today.month:02d}/day={today.day:02d}/"
    
    logger.info(f"Reading from: {input_path}")
    
    schema = define_schema()
    df = spark.read.schema(schema).option("header", "true").csv(input_path)
    
    record_count = df.count()
    logger.info(f"Read {record_count} raw records")
    
    if record_count == 0:
        logger.warning("No raw data found for processing")
        return None
    
    return df


def clean_and_prepare_data(df):
    """Clean and prepare data for analytics."""
    logger.info("Cleaning and preparing data...")
    
    df_clean = (df
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("change", F.col("change").cast(DoubleType()))
        .withColumn("change_percent", F.regexp_replace("change_percent", "%", ""))
        .withColumn("change_percent", F.col("change_percent").cast(DoubleType()))
        .withColumn("volume", F.col("volume").cast(IntegerType()))
        .filter(F.col("price").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("symbol").isNotNull()))
    
    clean_count = df_clean.count()
    logger.info(f"Cleaned data: {clean_count} records")
    
    return df_clean


def calculate_moving_averages(df):
    """Calculate moving averages and rolling statistics."""
    logger.info("Calculating moving averages and rolling statistics...")
    
    # Window specification for moving averages (partitioned by symbol, ordered by timestamp)
    window_spec_5 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-4, 0)   # 5-period MA
    window_spec_15 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-14, 0)  # 15-period MA
    window_spec_30 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-29, 0)  # 30-period MA
    
    df_with_ma = (df
        .withColumn("ma_5_periods", F.avg("price").over(window_spec_5))
        .withColumn("ma_15_periods", F.avg("price").over(window_spec_15))
        .withColumn("ma_30_periods", F.avg("price").over(window_spec_30))
        .withColumn("price_volatility_5", F.stddev("price").over(window_spec_5))
        .withColumn("price_volatility_15", F.stddev("price").over(window_spec_15))
        .withColumn("volume_ma_5", F.avg("volume").over(window_spec_5))
        .withColumn("volume_ma_15", F.avg("volume").over(window_spec_15)))
    
    logger.info("Moving averages calculated")
    return df_with_ma


def calculate_time_window_analytics(df):
    """Calculate time-based window analytics."""
    logger.info("Calculating time-window analytics...")
    
    # Add time windows
    df_windowed = (df
        .withColumn("window_5min", F.window(F.col("timestamp"), "5 minutes"))
        .withColumn("window_15min", F.window(F.col("timestamp"), "15 minutes"))
        .withColumn("window_1hour", F.window(F.col("timestamp"), "1 hour")))
    
    # 5-minute aggregations
    df_5min = (df_windowed
        .groupBy("symbol", "window_5min")
        .agg(
            F.first("timestamp").alias("window_start"),
            F.last("timestamp").alias("window_end"),
            F.avg("price").alias("avg_price_5m"),
            F.min("price").alias("min_price_5m"),
            F.max("price").alias("max_price_5m"),
            F.stddev("price").alias("volatility_5m"),
            F.sum("volume").alias("total_volume_5m"),
            F.count("*").alias("tick_count_5m"),
            F.first("ma_5_periods").alias("ma_5_periods"),
            F.first("ma_15_periods").alias("ma_15_periods"),
            F.first("price_volatility_5").alias("price_volatility_5")
        )
        .withColumn("window_type", F.lit("5min")))
    
    # 15-minute aggregations  
    df_15min = (df_windowed
        .groupBy("symbol", "window_15min")
        .agg(
            F.first("timestamp").alias("window_start"),
            F.last("timestamp").alias("window_end"),
            F.avg("price").alias("avg_price_15m"),
            F.min("price").alias("min_price_15m"),
            F.max("price").alias("max_price_15m"),
            F.stddev("price").alias("volatility_15m"),
            F.sum("volume").alias("total_volume_15m"),
            F.count("*").alias("tick_count_15m"),
            F.first("ma_15_periods").alias("ma_15_periods"),
            F.first("ma_30_periods").alias("ma_30_periods"),
            F.first("price_volatility_15").alias("price_volatility_15")
        )
        .withColumn("window_type", F.lit("15min")))
    
    # 1-hour aggregations
    df_1hour = (df_windowed
        .groupBy("symbol", "window_1hour")
        .agg(
            F.first("timestamp").alias("window_start"),
            F.last("timestamp").alias("window_end"),
            F.avg("price").alias("avg_price_1h"),
            F.min("price").alias("min_price_1h"),
            F.max("price").alias("max_price_1h"),
            F.stddev("price").alias("volatility_1h"),
            F.sum("volume").alias("total_volume_1h"),
            F.count("*").alias("tick_count_1h"),
            F.first("ma_30_periods").alias("ma_30_periods")
        )
        .withColumn("window_type", F.lit("1hour")))
    
    # Combine all time windows
    analytics_df = (df_15min
        .select("symbol", "window_start", "window_end", "window_type",
                "avg_price_15m", "min_price_15m", "max_price_15m", "volatility_15m", 
                "total_volume_15m", "tick_count_15m", "ma_15_periods", "ma_30_periods", "price_volatility_15")
        .union(df_5min.select("symbol", "window_start", "window_end", "window_type",
                             "avg_price_5m", "min_price_5m", "max_price_5m", "volatility_5m",
                             "total_volume_5m", "tick_count_5m", "ma_5_periods", "ma_15_periods", "price_volatility_5"))
        .union(df_1hour.select("symbol", "window_start", "window_end", "window_type",
                              "avg_price_1h", "min_price_1h", "max_price_1h", "volatility_1h",
                              "total_volume_1h", "tick_count_1h", "ma_30_periods", F.lit(None).alias("secondary_ma"), F.lit(None).alias("volatility_secondary"))))
    
    logger.info(f"Time-window analytics calculated: {analytics_df.count()} records")
    return analytics_df


def add_metadata_and_signals(df):
    """Add processing metadata and basic trading signals."""
    logger.info("Adding metadata and trading signals...")
    
    today = datetime.now()
    
    df_final = (df
        .withColumn("processing_time", F.current_timestamp())
        .withColumn("batch_id", F.lit(f"batch_{today.strftime('%Y%m%d_%H%M')}"))
        .withColumn("processing_date", F.lit(today.date()))
        
        # Simple trading signals
        .withColumn("price_trend", 
                   F.when(F.col("ma_15_periods") > F.col("ma_30_periods"), "bullish")
                    .when(F.col("ma_15_periods") < F.col("ma_30_periods"), "bearish")
                    .otherwise("neutral"))
        
        .withColumn("volatility_level",
                   F.when(F.col("volatility_15m") > 2.0, "high")
                    .when(F.col("volatility_15m") > 1.0, "medium")
                    .otherwise("low"))
        
        .withColumn("volume_trend",
                   F.when(F.col("total_volume_15m") > F.col("total_volume_1h") * 0.3, "high_activity")
                    .otherwise("normal_activity")))
    
    logger.info("Metadata and signals added")
    return df_final


def write_analytics_data(df, spark):
    """Write processed analytics data to S3/MinIO."""
    logger.info("Writing analytics data to MinIO...")
    
    today = datetime.now()
    output_path = f"s3a://{MINIO_BUCKET}/processed/realtime/year={today.year}/month={today.month:02d}/day={today.day:02d}/"
    
    logger.info(f"Writing to: {output_path}")
    
    # Write data partitioned by symbol and window_type
    (df
        .coalesce(2)  # Reduce partitions for efficiency
        .write
        .mode("overwrite")  # Overwrite existing data for this batch
        .partitionBy("symbol", "window_type")
        .option("compression", "snappy")
        .parquet(output_path))
    
    logger.info("Analytics data written successfully")


def main():
    """Main function for batch analytics processing."""
    logger.info("=" * 60)
    logger.info("STARTING STOCK MARKET BATCH ANALYTICS PROCESSING")
    logger.info("=" * 60)
    
    spark = None
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Read raw data
        raw_df = read_raw_data(spark)
        if raw_df is None:
            logger.warning("No data to process - exiting")
            return
        
        # Clean and prepare data
        clean_df = clean_and_prepare_data(raw_df)
        
        # Calculate moving averages
        ma_df = calculate_moving_averages(clean_df)
        
        # Calculate time-window analytics
        analytics_df = calculate_time_window_analytics(ma_df)
        
        # Add metadata and signals
        final_df = add_metadata_and_signals(analytics_df)
        
        # Write results
        write_analytics_data(final_df, spark)
        
        logger.info("Analytics processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in analytics processing: {e}")
        raise e
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")
        
        logger.info("=" * 60)
        logger.info("BATCH ANALYTICS PROCESSING COMPLETED")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()