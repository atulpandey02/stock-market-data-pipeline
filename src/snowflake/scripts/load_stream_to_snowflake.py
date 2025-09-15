#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import os
import traceback
from datetime import datetime, timedelta
import boto3
import numpy as np
import pandas as pd
import snowflake.connector
import io

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Configuration
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
S3_BUCKET = 'stock-market-data'

# Snowflake Configuration
SNOWFLAKE_ACCOUNT = 'KADNQRZ-CWC03608'
SNOWFLAKE_USER = 'ATULPANDEY02'
SNOWFLAKE_PASSWORD = 'Ankukaatul@123'
SNOWFLAKE_DATABASE = "STOCKMARKETSTREAM"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_TABLE = "DAILY_STREAM_METRICS"

def init_s3_client():
    """Initialize S3 client for MinIO."""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY
        )
        logger.info("S3/MinIO client initialized")
        return s3_client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        raise

def init_snowflake_connection():
    """Initialize Snowflake connection."""
    try:
        logger.info(f"Attempting Snowflake connection...")
        logger.info(f"Account: {SNOWFLAKE_ACCOUNT}")
        logger.info(f"User: {SNOWFLAKE_USER}")
        logger.info(f"Password length: {len(SNOWFLAKE_PASSWORD)} characters")
        
        if not SNOWFLAKE_PASSWORD:
            logger.warning("Snowflake password not set - using demo mode")
            return None
        
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        logger.info("Snowflake connection established successfully!")
        return conn
    except Exception as e:
        logger.error(f"Failed to establish Snowflake connection: {e}")
        logger.error(traceback.format_exc())
        return None

def create_analytics_table(conn):
    """Create analytics table that matches Spark output."""
    if not conn:
        logger.info("Demo mode: Would create analytics table")
        return
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        symbol STRING NOT NULL,
        window_start TIMESTAMP NOT NULL,
        window_15m_end TIMESTAMP,
        window_1h_end TIMESTAMP,
        ma_15m FLOAT,
        ma_1h FLOAT,
        volatility_15m FLOAT,
        volatility_1h FLOAT,
        volume_sum_15m BIGINT,
        volume_sum_1h BIGINT,
        processing_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        batch_id STRING,
        ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        PRIMARY KEY (symbol, window_start, batch_id)
    )
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Analytics table created/verified")
    except Exception as e:
        logger.error(f"Failed to create analytics table: {e}")
        raise
    finally:
        cursor.close()

def read_analytics_data():
    """Read processed analytics data from MinIO."""
    logger.info("Reading processed analytics data from MinIO...")
    
    s3_client = init_s3_client()
    today = datetime.now()
    
    s3_prefix = f"processed/realtime/year={today.year}/month={today.month:02d}/day={today.day:02d}"
    logger.info(f"Reading analytics data from: s3://{S3_BUCKET}/{s3_prefix}")
    
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
        
        if "Contents" not in response:
            logger.warning(f"No analytics data found for today: {today.date()}")
            return None
        
        all_dataframes = []
        
        for obj in response['Contents']:
            if obj['Key'].endswith(".parquet") and obj['Size'] > 0:
                logger.info(f"Reading file: {obj['Key']}")
                
                try:
                    # Read parquet file
                    response = s3_client.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
                    parquet_data = response['Body'].read()
                    parquet_buffer = io.BytesIO(parquet_data)
                    df = pd.read_parquet(parquet_buffer)
                    
                    logger.info(f"File columns: {df.columns.tolist()}")
                    logger.info(f"File shape: {df.shape}")
                    
                    all_dataframes.append(df)
                    
                except Exception as e:
                    logger.error(f"Error reading file {obj['Key']}: {e}")
                    continue
        
        if not all_dataframes:
            logger.warning("No valid analytics data found")
            return None
        
        # Combine all dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Read {len(combined_df)} analytics records from {len(all_dataframes)} files")
        logger.info(f"Combined dataframe columns: {combined_df.columns.tolist()}")
        
        # Minimal cleaning - just handle the actual columns we have
        combined_df = clean_analytics_data(combined_df)
        
        return combined_df
        
    except Exception as e:
        logger.error(f"Failed to read analytics data: {e}")
        logger.error(traceback.format_exc())
        return None

def clean_analytics_data(df):
    """Clean analytics data - FIXED to match actual Spark output."""
    logger.info("Cleaning analytics data...")
    logger.info(f"Input columns: {df.columns.tolist()}")
    
    # Add batch_id if missing
    if 'batch_id' not in df.columns:
        df['batch_id'] = f"batch_{datetime.now().strftime('%Y%m%d_%H%M')}"
    
    # Add processing_time if missing
    if 'processing_time' not in df.columns:
        df['processing_time'] = pd.Timestamp.now()
    
    # Convert timestamp columns
    timestamp_columns = ['window_start', 'window_15m_end', 'window_1h_end', 'processing_time']
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    # Ensure symbol is string
    if 'symbol' in df.columns:
        df['symbol'] = df['symbol'].astype(str)
    
    # Remove duplicates based on key columns
    key_columns = ['symbol', 'window_start']
    available_key_columns = [col for col in key_columns if col in df.columns]
    if available_key_columns:
        df = df.drop_duplicates(subset=available_key_columns, keep='last')
    
    # Fill NaN values
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)
    
    string_columns = df.select_dtypes(include=[object]).columns
    df[string_columns] = df[string_columns].fillna('unknown')
    
    logger.info(f"Cleaned analytics data: {len(df)} records")
    logger.info(f"Final columns: {df.columns.tolist()}")
    
    return df

def load_analytics_to_snowflake(conn, df):
    """Load analytics data to Snowflake - FIXED column mapping."""
    if df is None or df.empty:
        logger.info("No analytics data to load")
        return
    
    if not conn:
        logger.info("Demo mode: Analytics data summary")
        logger.info(f"Records: {len(df)}")
        logger.info(f"Columns: {df.columns.tolist()}")
        if 'symbol' in df.columns:
            logger.info(f"Symbols: {df['symbol'].nunique()}")
            logger.info(f"Unique symbols: {df['symbol'].unique().tolist()}")
        
        # Show sample data
        if len(df) > 0:
            sample = df.iloc[0]
            logger.info("Sample record:")
            for col in df.columns:
                logger.info(f"  {col}: {sample[col]}")
        
        logger.info("Demo mode: Would load to Snowflake DAILY_STREAM_METRICS table")
        return
    
    try:
        cursor = conn.cursor()
        
        # Create temporary staging table
        stage_table = "TEMP_ANALYTICS_STAGE"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {stage_table} LIKE {SNOWFLAKE_TABLE}")
        
        # Prepare records for insertion - only use columns that exist
        records = []
        for _, row in df.iterrows():
            record = (
                str(row.get('symbol', '')),
                row.get('window_start'),
                row.get('window_15m_end'),
                row.get('window_1h_end'),
                float(row.get('ma_15m', 0)) if pd.notna(row.get('ma_15m')) else None,
                float(row.get('ma_1h', 0)) if pd.notna(row.get('ma_1h')) else None,
                float(row.get('volatility_15m', 0)) if pd.notna(row.get('volatility_15m')) else None,
                float(row.get('volatility_1h', 0)) if pd.notna(row.get('volatility_1h')) else None,
                int(row.get('volume_sum_15m', 0)) if pd.notna(row.get('volume_sum_15m')) else None,
                int(row.get('volume_sum_1h', 0)) if pd.notna(row.get('volume_sum_1h')) else None,
                row.get('processing_time'),
                str(row.get('batch_id', ''))
            )
            records.append(record)
        
        # Insert into staging table
        insert_query = f"""
        INSERT INTO {stage_table} 
        (symbol, window_start, window_15m_end, window_1h_end, ma_15m, ma_1h, 
         volatility_15m, volatility_1h, volume_sum_15m, volume_sum_1h, processing_time, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        logger.info(f"Inserting {len(records)} records into staging table")
        cursor.executemany(insert_query, records)
        
        # Merge into main table
        merge_query = f"""
        MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
        USING {stage_table} AS source
        ON target.symbol = source.symbol 
           AND target.window_start = source.window_start 
           AND target.batch_id = source.batch_id
        WHEN MATCHED THEN
            UPDATE SET
                target.window_15m_end = source.window_15m_end,
                target.window_1h_end = source.window_1h_end,
                target.ma_15m = source.ma_15m,
                target.ma_1h = source.ma_1h,
                target.volatility_15m = source.volatility_15m,
                target.volatility_1h = source.volatility_1h,
                target.volume_sum_15m = source.volume_sum_15m,
                target.volume_sum_1h = source.volume_sum_1h,
                target.processing_time = source.processing_time,
                target.ingestion_time = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (symbol, window_start, window_15m_end, window_1h_end, ma_15m, ma_1h,
                   volatility_15m, volatility_1h, volume_sum_15m, volume_sum_1h, processing_time, batch_id)
            VALUES (source.symbol, source.window_start, source.window_15m_end, source.window_1h_end,
                   source.ma_15m, source.ma_1h, source.volatility_15m, source.volatility_1h,
                   source.volume_sum_15m, source.volume_sum_1h, source.processing_time, source.batch_id)
        """
        
        logger.info("Executing MERGE statement...")
        cursor.execute(merge_query)
        conn.commit()
        logger.info(f"Successfully loaded {len(records)} analytics records to Snowflake")
        
        # Log summary statistics
        cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")
        total_records = cursor.fetchone()[0]
        logger.info(f"Total records in Snowflake table: {total_records}")
        
    except Exception as e:
        logger.error(f"Failed to load analytics data to Snowflake: {e}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if conn:
            cursor.close()

def main():
    """Main function for loading analytics data to Snowflake."""
    logger.info("=" * 60)
    logger.info("STARTING SNOWFLAKE ANALYTICS DATA LOAD (FIXED VERSION)")
    logger.info("=" * 60)
    
    conn = None
    try:
        # Initialize connections
        conn = init_snowflake_connection()
        
        # Create/verify table
        create_analytics_table(conn)
        
        # Read processed analytics data
        analytics_df = read_analytics_data()
        
        # Load to Snowflake
        load_analytics_to_snowflake(conn, analytics_df)
        
        logger.info("Analytics data load completed successfully!")
        
    except Exception as e:
        logger.error(f"Analytics data load failed: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Snowflake connection closed")
        
        logger.info("=" * 60)
        logger.info("ANALYTICS DATA LOAD COMPLETED")
        logger.info("=" * 60)

if __name__ == "__main__":
    main()