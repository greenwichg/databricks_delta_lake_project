# Databricks notebook source

# MAGIC %md
# MAGIC # Workflow: Streaming Pipeline Orchestration
# MAGIC
# MAGIC This notebook manages the **continuous streaming pipelines** that provide
# MAGIC near-real-time data processing. Unlike the daily batch pipeline, streaming
# MAGIC pipelines run continuously and process data as it arrives.
# MAGIC
# MAGIC ## Streaming vs Batch Orchestration
# MAGIC
# MAGIC | Aspect | Batch (Daily) | Streaming |
# MAGIC |--------|--------------|-----------|
# MAGIC | Trigger | Scheduled (cron) | Continuous |
# MAGIC | Latency | Hours | Seconds-Minutes |
# MAGIC | Cost | Lower (runs periodically) | Higher (always running) |
# MAGIC | Error handling | Retry entire job | Stream auto-retries |
# MAGIC | Monitoring | Job success/failure | Throughput, lag, backlog |

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Streaming Pipelines

# COMMAND ----------

def start_streaming_pipelines():
    """
    Start all streaming pipelines for the Customer 360 platform.

    In production, each stream would be a separate Databricks Job
    with 'continuous' trigger mode.
    """

    print("Starting streaming pipelines...")

    # Pipeline 1: Real-time transaction processing
    print("\n1. Starting real-time transaction stream...")
    # %run ../src/streaming/stream_transactions
    print("   Transaction stream started.")

    # Pipeline 2: Real-time clickstream processing
    print("\n2. Starting real-time clickstream stream...")
    # %run ../src/streaming/stream_clickstream
    print("   Clickstream stream started.")

    print(f"\nAll streams started at {datetime.now().isoformat()}")
    print("Monitor via: spark.streams.active")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Streaming Queries

# COMMAND ----------

def monitor_streams():
    """
    Monitor all active streaming queries.
    Shows throughput, latency, and backlog for each stream.
    """

    active_streams = spark.streams.active

    if not active_streams:
        print("No active streaming queries.")
        return

    for stream in active_streams:
        progress = stream.lastProgress

        if progress:
            print(f"\nStream: {stream.name or stream.id}")
            print(f"  Status: {'Active' if stream.isActive else 'Stopped'}")
            print(f"  Batch ID: {progress.get('batchId', 'N/A')}")
            print(f"  Input rows/sec: {progress.get('inputRowsPerSecond', 0):.1f}")
            print(f"  Processed rows/sec: {progress.get('processedRowsPerSecond', 0):.1f}")
            print(f"  Batch records: {progress.get('numInputRows', 0)}")

            # Check for backlog (input > processed = falling behind)
            input_rate = progress.get('inputRowsPerSecond', 0)
            process_rate = progress.get('processedRowsPerSecond', 0)
            if input_rate > process_rate * 1.5 and input_rate > 0:
                print(f"  WARNING: Stream is falling behind!")
        else:
            print(f"\nStream {stream.id}: No progress data yet")

# COMMAND ----------

def stop_all_streams():
    """Gracefully stop all streaming queries."""
    for stream in spark.streams.active:
        print(f"Stopping stream: {stream.id}")
        stream.stop()
    print("All streams stopped.")

# COMMAND ----------

# start_streaming_pipelines()
# monitor_streams()
# stop_all_streams()
