import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

# Simple MinIO Data Sensor
class MinIODataSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, bucket_name, prefix, min_files=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.min_files = min_files

    def poke(self, context):
        try:
            from minio import Minio
            from datetime import datetime
            
            client = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
            today = datetime.now()
            actual_prefix = f"{self.prefix}/year={today.year}/month={today.month:02d}/day={today.day:02d}"
            
            objects = list(client.list_objects(self.bucket_name, prefix=actual_prefix, recursive=True))
            data_files = [obj for obj in objects if obj.size > 0]
            
            # Also check for symbol folders (for processed data)
            symbol_folders = set()
            for obj in objects:
                if 'symbol=' in obj.object_name:
                    parts = obj.object_name.split('/')
                    for part in parts:
                        if part.startswith('symbol='):
                            symbol_folders.add(part)
            
            self.log.info(f"Found {len(data_files)} files and {len(symbol_folders)} symbol folders in {actual_prefix}")
            return len(data_files) >= self.min_files or len(symbol_folders) >= 1
            
        except Exception as e:
            self.log.error(f"Error checking MinIO: {e}")
            return False

# DAG Configuration
default_args = {
    "owner": "Atul",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

dag = DAG(
    "stock_streaming_pipeline",
    default_args=default_args,
    description="Clean Stock Streaming Analytics Pipeline",
    schedule_interval=timedelta(days=1), #timedelta(minutes=30)
    start_date=datetime(2025, 9, 15),
    max_active_runs=1,
    tags=['streaming', 'analytics', 'clean', 'snowflake']
)

# ===== TASK 1: CLEANUP =====
cleanup_processes = BashOperator(
    task_id="cleanup_processes",
    bash_command="""
    echo "Cleaning up existing processes..."
    pkill -9 -f "stream_data_producer.py" 2>/dev/null || true
    pkill -9 -f "realtime_data_consumer.py" 2>/dev/null || true
    docker exec stockmarketdatapipeline-spark-master-1 pkill -9 -f "spark" 2>/dev/null || true
    docker exec stockmarketdatapipeline-spark-master-1 rm -rf /tmp/spark-* 2>/dev/null || true
    sleep 3
    echo "Cleanup completed"
    """,
    dag=dag,
)

# ===== TASK 2: COLLECT STREAMING DATA =====
collect_streaming_data = BashOperator(
    task_id="collect_streaming_data",
    bash_command="""
    echo "Starting 3-minute streaming data collection..."
    
    # Start producer and consumer with 3-minute timeout
    timeout 180 python /opt/airflow/dags/scripts/stream_data_producer.py {{ ds }} &
    PRODUCER_PID=$!
    
    timeout 180 python /opt/airflow/dags/scripts/realtime_data_consumer.py {{ ds }} &
    CONSUMER_PID=$!
    
    echo "Producer ($PRODUCER_PID) and Consumer ($CONSUMER_PID) started"
    
    # Wait for both processes
    wait $PRODUCER_PID
    wait $CONSUMER_PID
    
    # Force cleanup any remaining processes
    pkill -f "stream_data_producer.py" 2>/dev/null || true
    pkill -f "realtime_data_consumer.py" 2>/dev/null || true
    
    echo "Streaming data collection completed"
    """,
    dag=dag,
)

# ===== TASK 3: WAIT FOR RAW DATA =====
wait_for_raw_data = MinIODataSensor(
    task_id='wait_for_raw_data',
    bucket_name='stock-market-data',
    prefix='raw/realtime',
    min_files=1,
    poke_interval=15,
    timeout=180,
    dag=dag
)

# ===== TASK 4: SPARK ANALYTICS PROCESSING =====
spark_analytics_processing = BashOperator(
    task_id="spark_analytics_processing",
    bash_command="""
    echo "Starting Spark analytics processing..."
    
    # Run the standalone Spark analytics script with timeout
    timeout 300 docker exec stockmarketdatapipeline-spark-master-1 \
        spark-submit \
            --master spark://spark-master:7077 \
            --driver-memory 1g \
            --executor-memory 1g \
            --executor-cores 1 \
            --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
            /opt/spark/jobs/spark_stream_batch_processor.py
    
    EXIT_CODE=$?
    
    # Cleanup Spark processes
    docker exec stockmarketdatapipeline-spark-master-1 pkill -9 -f "spark_batch_analytics" 2>/dev/null || true
    
    if [ $EXIT_CODE -eq 0 ] || [ $EXIT_CODE -eq 124 ]; then
        echo "Spark analytics processing completed successfully"
        exit 0
    else
        echo "Spark analytics processing failed with exit code: $EXIT_CODE"
        exit 1
    fi
    """,
    dag=dag,
)

# ===== TASK 5: VALIDATE ANALYTICS DATA =====
validate_analytics_data = MinIODataSensor(
    task_id='validate_analytics_data',
    bucket_name='stock-market-data',
    prefix='processed/realtime',
    min_files=1,
    poke_interval=15,
    timeout=180,
    dag=dag
)

# ===== TASK 6: LOAD TO SNOWFLAKE =====
load_to_snowflake = BashOperator(
    task_id="load_to_snowflake",
    bash_command="""
    echo "Loading analytics data to Snowflake..."
    
    # Set Snowflake password from Airflow Variables
    export SNOWFLAKE_PASSWORD="{{ var.value.get('snowflake_password', '') }}"
    
    # Run the standalone Snowflake loader script
    cd /opt/airflow/dags/scripts
    python load_stream_to_snowflake.py
    
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Snowflake load completed successfully"
    else
        echo "Snowflake load failed with exit code: $EXIT_CODE"
        exit $EXIT_CODE
    fi
    """,
    dag=dag,
)

# ===== TASK 7: PIPELINE SUMMARY =====
pipeline_summary = BashOperator(
    task_id="pipeline_summary",
    bash_command="""
    echo "=== PIPELINE EXECUTION SUMMARY ==="
    
    # Check for any remaining processes
    PRODUCER_COUNT=$(pgrep -f "stream_data_producer.py" | wc -l)
    CONSUMER_COUNT=$(pgrep -f "realtime_data_consumer.py" | wc -l)
    
    echo "Remaining producer processes: $PRODUCER_COUNT"
    echo "Remaining consumer processes: $CONSUMER_COUNT"
    
    # Check data in MinIO using Python
    python -c "
from minio import Minio
from datetime import datetime

try:
    client = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
    today = datetime.now()
    
    # Check raw data
    raw_prefix = f'raw/realtime/year={today.year}/month={today.month:02d}/day={today.day:02d}'
    raw_objects = list(client.list_objects('stock-market-data', prefix=raw_prefix, recursive=True))
    raw_files = [obj for obj in raw_objects if obj.object_name.endswith('.csv') and obj.size > 0]
    
    # Check processed data
    processed_prefix = f'processed/realtime/year={today.year}/month={today.month:02d}/day={today.day:02d}'
    processed_objects = list(client.list_objects('stock-market-data', prefix=processed_prefix, recursive=True))
    processed_files = [obj for obj in processed_objects if obj.object_name.endswith('.parquet') and obj.size > 0]
    
    # Count symbol folders
    symbol_folders = set()
    for obj in processed_objects:
        if 'symbol=' in obj.object_name:
            parts = obj.object_name.split('/')
            for part in parts:
                if part.startswith('symbol='):
                    symbol_folders.add(part)
    
    print(f'Raw CSV files: {len(raw_files)}')
    print(f'Processed parquet files: {len(processed_files)}')
    print(f'Symbol folders: {len(symbol_folders)}')
    print(f'Symbols processed: {list(symbol_folders)}')
    
    if len(raw_files) > 0 and len(processed_files) > 0:
        print('✅ Pipeline executed successfully - Data flow complete')
    else:
        print('⚠️  Pipeline may have issues - Check individual task logs')
        
except Exception as e:
    print(f'❌ Error checking pipeline status: {e}')
"
    
    echo "=== PIPELINE SUMMARY COMPLETE ==="
    """,
    dag=dag,
)

# ===== FINAL CLEANUP =====
final_cleanup = BashOperator(
    task_id="final_cleanup",
    bash_command="""
    echo "Final pipeline cleanup..."
    
    # Kill any remaining processes
    pkill -9 -f "stream_data_producer.py" 2>/dev/null || true
    pkill -9 -f "realtime_data_consumer.py" 2>/dev/null || true
    docker exec stockmarketdatapipeline-spark-master-1 pkill -9 -f "spark" 2>/dev/null || true
    
    # Clean temporary files
    docker exec stockmarketdatapipeline-spark-master-1 rm -rf /tmp/spark-* 2>/dev/null || true
    
    echo "Stock market streaming analytics pipeline completed!"
    """,
    trigger_rule='all_done',  # Run regardless of upstream success/failure
    dag=dag,
)

# ===== TASK DEPENDENCIES =====
cleanup_processes >> collect_streaming_data >> wait_for_raw_data >> spark_analytics_processing >> validate_analytics_data >> load_to_snowflake >> pipeline_summary >> final_cleanup