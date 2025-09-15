import json 
import logging
import os
import time
from datetime import datetime

import pandas as pd 
import numpy as np

from confluent_kafka import Consumer
from minio import Minio
from minio.error import S3Error

from dotenv import load_dotenv

load_dotenv()

#Configure Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC_BATCH = "stock-market-batch"
KAFKA_GROUP_ID =  "stock-market-batch-consumer-group"

# Timeout configuration
CONSUMER_TIMEOUT_SECONDS = 30  # Exit after 30 seconds of no messages
MAX_RUNTIME_SECONDS = 300      # Maximum runtime (5 minutes)

#MinIO configuration
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "stock-market-data"
MINIO_ENDPOINT = "minio:9000"

def create_minio_client():
    """Initialize MinIO Client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ensure_bucket_exists(minio_client, bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
        else:
            logger.info(f"Bucket {bucket_name} already exists")
    except S3Error as e:
        logger.error(f"Error creating bucket {bucket_name}: {e}")
        raise

def main():
    # Create a MinIO client
    minio_client = create_minio_client()
    ensure_bucket_exists(minio_client, MINIO_BUCKET)

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC_BATCH])

    logger.info(f"Starting consumer for topic {KAFKA_TOPIC_BATCH}")
    
    start_time = time.time()
    last_message_time = time.time()
    messages_processed = 0

    try:
        while True:
            # Check for maximum runtime
            if time.time() - start_time > MAX_RUNTIME_SECONDS:
                logger.info(f"Maximum runtime of {MAX_RUNTIME_SECONDS} seconds reached")
                break
            
            # Check for timeout since last message
            if time.time() - last_message_time > CONSUMER_TIMEOUT_SECONDS:
                logger.info(f"No messages received for {CONSUMER_TIMEOUT_SECONDS} seconds. Assuming all data consumed.")
                break

            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                last_message_time = time.time()  # Reset timeout
                messages_processed += 1
                
                data = json.loads(msg.value().decode("utf-8"))
                print(data)
                symbol = data['symbol']
                date = data['batch_date']

                year, month, day = date.split("-")

                df = pd.DataFrame([data])

                #Save to minio
                object_name = f"raw/historical/year={year}/month={month}/day={day}/{symbol}_{datetime.now().strftime('%H%M%S')}.csv"
                parquet_file = f"/tmp/{symbol}.csv"
                df.to_csv(parquet_file, index=False)

                minio_client.fput_object(
                    MINIO_BUCKET,
                    object_name,
                    parquet_file,
                )
                logger.info(f"Wrote data for {symbol} to s3://{MINIO_BUCKET}/{object_name}")

                os.remove(parquet_file)
                consumer.commit()
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Stopping consumer due to keyboard interrupt")
    finally:
        consumer.close()
        logger.info(f"Consumer finished. Processed {messages_processed} messages in {time.time() - start_time:.2f} seconds")
        
    return messages_processed

if __name__ == "__main__":
    main()