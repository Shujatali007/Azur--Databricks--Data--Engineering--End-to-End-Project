# 🌊 Spark Structured Streaming

This folder contains implementations of **Spark Structured Streaming** for processing real-time data streams in the pricing analytics platform. These notebooks demonstrate how to build scalable, fault-tolerant streaming ETL pipelines.

## 📋 Overview

Spark Structured Streaming provides a high-level API for stream processing, treating data streams as unbounded tables. This collection demonstrates:
- Reading from streaming sources
- Stream transformations and processing
- Writing to streaming sinks with checkpointing
- Monitoring and managing streaming queries
- Trigger modes and output modes
- Fault tolerance and exactly-once processing

---

## 📂 Files in This Folder

### 1️⃣ **01-Ingest-Daily-Pricing-Streaming-Source-Data.sql**
**Purpose**: Generate streaming source data from batch data for testing streaming pipelines

**Topics Covered:**
- ✅ Creating streaming source data
- ✅ Data extraction from Delta tables
- ✅ Adding streaming timestamps
- ✅ Writing to JSON streaming source
- ✅ Data simulation for real-time scenarios

**Key Implementation:**

#### **Generate Streaming Source Data**

```python
# Define streaming source path
dailyPricingStramingSourceFolderPath = "abfss://bronze@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing-streaming-source-data"

# Use catalog
USE CATALOG pricing_analytics;

# Extract data with streaming timestamp
dailyPricingSourceStreamDF = spark.sql("""
    SELECT
        current_timestamp() as DATETIME_OF_PRICING,
        cast(ROW_ID as bigint),
        STATE_NAME,
        MARKET_NAME,
        PRODUCTGROUP_NAME,
        PRODUCT_NAME,
        VARIETY,
        ORIGIN,
        cast(ARRIVAL_IN_TONNES as decimal(18,2)) + extract(seconds from current_timestamp())/100 as ARRIVAL_IN_TONNES, 
        cast(MINIMUM_PRICE as decimal(36,2)) + extract(seconds from current_timestamp())/100 as MINIMUM_PRICE,
        cast(MAXIMUM_PRICE as decimal(36,2)) + extract(seconds from current_timestamp())/100 as MAXIMUM_PRICE,
        cast(MODAL_PRICE as decimal(36,2)) + extract(seconds from current_timestamp())/100 as MODAL_PRICE,
        current_timestamp() as source_stream_load_datetime
    FROM bronze.daily_pricing
    WHERE DATE_OF_PRICING='01/01/2023'
""")

# Write to streaming source location
(dailyPricingSourceStreamDF
    .write
    .mode('append')
    .json(dailyPricingStramingSourceFolderPath)
)

# Verify files created
dbutils.fs.ls(dailyPricingStramingSourceFolderPath)
```

**Key Features:**
- **Dynamic Timestamps**: Uses `current_timestamp()` to simulate real-time data
- **Price Variation**: Adds seconds-based variation to prices for realistic streaming
- **JSON Format**: Writes data in JSON format for streaming consumption
- **Append Mode**: Continuously adds new data to simulate incoming stream

**Use Cases:**
- Testing streaming pipelines locally
- Simulating real-time data ingestion
- Development and debugging
- Load testing streaming applications

---

### 2️⃣ **01-Processing-Streaming-Source-Data.py**
**Purpose**: Complete streaming ETL pipeline with reading, processing, and writing streams

**Topics Covered:**
- ✅ DataStreamReader for reading streams
- ✅ DataStreamWriter for writing streams
- ✅ Checkpoint management
- ✅ Query monitoring and control
- ✅ Output modes (append, complete, update)
- ✅ Trigger configurations
- ✅ StreamingQuery lifecycle management

**Schema Definition:**
```python
streaming_schema = """
    ARRIVAL_IN_TONNES double,
    DATETIME_OF_PRICING string,
    MARKET_NAME string,
    MAXIMUM_PRICE double,
    MINIMUM_PRICE double,
    MODAL_PRICE double,
    ORIGIN string,
    PRODUCTGROUP_NAME string,
    PRODUCT_NAME string,
    ROW_ID long,
    STATE_NAME string,
    VARIETY string,
    source_stream_load_datetime string
"""
```

**Complete Streaming Pipeline:**

#### **Step 1: Define Source and Sink Paths**

```python
# Source: Where streaming data arrives
sourceStreamJSONFilePath = 'abfss://bronze@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing-streaming-source-data'

# Sink: Where processed stream is written
sinkStreamJSONFilePath = 'abfss://bronze@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing-streaming-data/json'

# Checkpoint: For fault tolerance
sinkStreamJSONcheckpointPath = 'abfss://bronze@adlsujadatalakehousedev.dfs.core.windows.net/daily-pricing-streaming-data/json/checkpoint'
```

#### **Step 2: Create Streaming Reader**

```python
# Read stream with explicit schema
sourceStreamJSONFileDF = (
    spark
    .readStream
    .schema(streaming_schema)
    .format("json")
    .load(sourceStreamJSONFilePath)
)

# Note: Avoid inferSchema with streaming - always define schema explicitly
```

#### **Step 3: Basic Stream Writer (Without Checkpoint)**

```python
# Simple streaming write (not recommended for production)
(sourceStreamJSONFileDF
    .writeStream
    .format("json")
    .start(sinkStreamJSONFilePath)
)

# WARNING: No fault tolerance - if query fails, may lose data
```

#### **Step 4: Production-Ready Stream Writer (With Checkpoint)**

```python
# Create streaming query with full configuration
streamProcessingQuery = (
    sourceStreamJSONFileDF
    .writeStream
    .outputMode("append")              # Append new records only
    .format("json")                     # Output format
    .queryName("stream-processing")     # Query identifier
    .trigger(processingTime="5 Minutes") # Micro-batch every 5 minutes
    .option("checkpointLocation", sinkStreamJSONcheckpointPath)  # Fault tolerance
    .start(sinkStreamJSONFilePath)
)
```

#### **Step 5: Monitor Streaming Query**

```python
# Get unique query ID
query_id = streamProcessingQuery.id
print(f"Query ID: {query_id}")

# Check query status
status = streamProcessingQuery.status
print(f"Status: {status}")
# Returns: {'message': 'Processing', 'isDataAvailable': True, 'isTriggerActive': True}

# Get last progress information
last_progress = streamProcessingQuery.lastProgress
print(f"Last Progress: {last_progress}")
# Shows: records processed, batch duration, input rates, etc.
```

#### **Step 6: Query Batch Results (Testing)**

```python
# Read processed stream data as batch
StreamDF = (
    spark
    .read
    .format("json")
    .load(sinkStreamJSONFilePath)
)

# Check record count
display(StreamDF.count())

# View sample data
display(StreamDF.limit(100))
```

#### **Step 7: Stop Streaming Query**

```python
# Gracefully stop the streaming query
streamProcessingQuery.stop()

# Or stop all active streams
for stream in spark.streams.active:
    stream.stop()
```

**Structured Streaming Core Classes:**

| Class | Purpose | Key Methods |
|-------|---------|-------------|
| **DataStreamReader** | Read from streaming sources | `.readStream`, `.schema()`, `.format()`, `.load()` |
| **DataStreamWriter** | Write to streaming sinks | `.writeStream`, `.outputMode()`, `.trigger()`, `.start()` |
| **StreamingQuery** | Manage running queries | `.id`, `.status`, `.lastProgress`, `.stop()` |

**Output Modes:**

| Mode | Description | Use Case |
|------|-------------|----------|
| **Append** | Only new rows added to result | Default for most operations, event logs |
| **Complete** | Entire result table rewritten | Aggregations, real-time dashboards |
| **Update** | Only updated rows output | Stateful operations, changing aggregates |

**Trigger Types:**

```python
# Process as data arrives (continuous)
.trigger(continuous="1 second")

# Fixed interval micro-batches
.trigger(processingTime="5 Minutes")

# Process once (for testing)
.trigger(once=True)

# Process all available data then stop
.trigger(availableNow=True)
```

**Use Cases:**
- Real-time data ingestion
- Continuous ETL pipelines
- Stream-to-stream joins
- Real-time analytics
- Event processing

---

### 3️⃣ **Spark-Structured-Streaming-Introduction.txt**
**Purpose**: Quick reference guide with streaming concepts and APIs

**Contents:**
- Structured Streaming API documentation links
- Schema definitions
- File paths and configurations
- Code snippets for common patterns

---

### 4️⃣ **Streaming-Reader-Writer-Final.txt**
**Purpose**: Complete code reference for streaming reader/writer patterns

**Contents:**
- Full streaming pipeline code
- Checkpoint configuration
- Query monitoring code
- Best practices implementation

---

## 🎯 Common Streaming Patterns

### **Pattern 1: File-to-File Streaming**

```python
# Read from cloud storage, write to cloud storage
streamDF = (
    spark.readStream
    .format("json")
    .schema(mySchema)
    .load("abfss://input/path/")
)

(streamDF
    .writeStream
    .format("delta")
    .option("checkpointLocation", "abfss://checkpoint/path/")
    .start("abfss://output/path/")
)
```

### **Pattern 2: Stream Transformations**

```python
# Apply transformations to stream
transformedStream = (
    sourceStream
    .filter(col("MODAL_PRICE") > 100)
    .withColumn("price_category", 
                when(col("MODAL_PRICE") > 500, "High")
                .when(col("MODAL_PRICE") > 200, "Medium")
                .otherwise("Low"))
    .select("STATE_NAME", "PRODUCT_NAME", "MODAL_PRICE", "price_category")
)

(transformedStream
    .writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .start(output_path)
)
```

### **Pattern 3: Windowed Aggregations**

```python
from pyspark.sql.functions import window, avg, count

# Aggregate over time windows
windowedAggregates = (
    streamDF
    .withWatermark("DATETIME_OF_PRICING", "10 minutes")  # Handle late data
    .groupBy(
        window("DATETIME_OF_PRICING", "5 minutes"),
        "PRODUCT_NAME"
    )
    .agg(
        avg("MODAL_PRICE").alias("avg_price"),
        count("*").alias("record_count")
    )
)

(windowedAggregates
    .writeStream
    .outputMode("update")  # Use update for aggregations
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .start(output_path)
)
```

### **Pattern 4: Stream-to-Delta Table**

```python
# Write stream directly to Delta table
(streamDF
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")  # Handle schema evolution
    .toTable("pricing_analytics.bronze.daily_pricing_stream")
)
```

### **Pattern 5: Foreachbatch for Custom Processing**

```python
def process_batch(batch_df, batch_id):
    """Custom processing logic for each micro-batch"""
    print(f"Processing batch {batch_id}")
    
    # Apply complex transformations
    processed_df = batch_df.transform(custom_transformation)
    
    # Write to multiple sinks
    processed_df.write.format("delta").mode("append").save(path1)
    processed_df.write.format("parquet").mode("append").save(path2)
    
    # Update metrics
    record_count = processed_df.count()
    print(f"Processed {record_count} records in batch {batch_id}")

# Use foreachBatch for custom processing
(streamDF
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_path)
    .start()
)
```

---

## 🔧 Checkpoint Management

**What is Checkpointing?**
Checkpointing enables fault tolerance by saving the current state of the stream processing:
- **Offset Tracking**: Tracks which data has been processed
- **State Management**: Stores aggregation state for stateful operations
- **Fault Recovery**: Resumes from last checkpoint after failure

**Checkpoint Location:**
```python
checkpoint_path = "abfss://container@storage.dfs.core.windows.net/checkpoints/query-name/"

.option("checkpointLocation", checkpoint_path)
```

**Important Notes:**
- ⚠️ **Never delete checkpoint directories** while query is running
- ⚠️ **Schema changes** may require new checkpoint location
- ⚠️ **Different queries** must have different checkpoint paths
- ✅ **Checkpoint path must be reliable** (cloud storage, not local)

---

## 📊 Monitoring Streaming Queries

### **Query Status**

```python
# Get all active streams
active_streams = spark.streams.active
for stream in active_streams:
    print(f"Query: {stream.name}, Status: {stream.status}")

# Check specific query
query = streamProcessingQuery
print(f"Is Active: {query.isActive}")
print(f"Query ID: {query.id}")
print(f"Run ID: {query.runId}")
```

### **Progress Metrics**

```python
# Get detailed progress information
progress = query.lastProgress

if progress:
    print(f"Batch ID: {progress['batchId']}")
    print(f"Records Processed: {progress['numInputRows']}")
    print(f"Batch Duration: {progress['batchDuration']} ms")
    print(f"Input Rate: {progress['inputRowsPerSecond']} rows/sec")
    print(f"Processing Rate: {progress['processedRowsPerSecond']} rows/sec")
```

### **Recent Progress History**

```python
# Get last 10 batches
recent_progress = query.recentProgress
for batch in recent_progress:
    print(f"Batch {batch['batchId']}: {batch['numInputRows']} rows")
```

---

## 🚀 Performance Optimization

### **1. Schema Definition**
```python
# ✅ GOOD: Explicit schema (fast, predictable)
.schema("col1 STRING, col2 INT, col3 DOUBLE")

# ❌ BAD: Schema inference (slow, unreliable)
.option("inferSchema", "true")  # Don't use with streaming!
```

### **2. Trigger Configuration**
```python
# For low latency (seconds delay)
.trigger(processingTime="10 seconds")

# For cost optimization (larger batches)
.trigger(processingTime="5 minutes")

# For exactly-once processing
.trigger(availableNow=True)
```

### **3. Partitioning**
```python
# Partition output for better query performance
(streamDF
    .writeStream
    .partitionBy("STATE_NAME", "year", "month")
    .format("delta")
    .start(output_path)
)
```

### **4. Watermarking for Late Data**
```python
# Handle data arriving late
(streamDF
    .withWatermark("event_time", "1 hour")  # Wait up to 1 hour for late data
    .groupBy(window("event_time", "10 minutes"), "product")
    .count()
)
```

---

## 🛠️ Troubleshooting

### **Common Issues:**

#### **Issue 1: Schema Mismatch**
```python
# Error: Schema mismatch between source and checkpoint

# Solution: Use new checkpoint location or update schema
.option("mergeSchema", "true")
```

#### **Issue 2: Query Not Processing**
```python
# Check if data is arriving
dbutils.fs.ls(source_path)

# Check query status
query.status  # Should show 'isDataAvailable': True
```

#### **Issue 3: Checkpoint Errors**
```python
# Error: Checkpoint directory not accessible

# Solution: Verify path and permissions
dbutils.fs.ls(checkpoint_path)
```

#### **Issue 4: Memory Issues**
```python
# Configure memory for streaming
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

---

## 📚 Key Concepts

### **Structured Streaming Model**
- Treats streams as unbounded tables
- New data = new rows appended to table
- Queries run continuously on new data
- Results updated incrementally

### **Fault Tolerance**
- **Checkpointing**: State persistence
- **Exactly-once semantics**: No duplicates
- **Automatic recovery**: Resume from checkpoint

### **State Management**
- **Stateful operations**: Aggregations, joins, deduplication
- **State store**: Maintains state across micro-batches
- **State cleanup**: Watermarking for bounded state

---

## 🔗 Additional Resources

- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [DataStreamReader API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html)
- [DataStreamWriter API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html)
- [StreamingQuery API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html)
- [Delta Lake Streaming](https://docs.delta.io/latest/delta-streaming.html)

---

## ✅ Learning Checklist

After completing these notebooks, you should be able to:

- [ ] Create streaming DataFrames from various sources
- [ ] Define schemas for streaming data
- [ ] Configure checkpointing for fault tolerance
- [ ] Write streams to different sinks (Delta, JSON, Parquet)
- [ ] Monitor and manage streaming queries
- [ ] Implement windowed aggregations
- [ ] Handle late-arriving data with watermarks
- [ ] Optimize streaming performance
- [ ] Troubleshoot common streaming issues
- [ ] Use foreachBatch for custom processing

---

**🔗 Related Sections:**
- [Ingestion Layer](../01-Ingestion/README.md) - Batch ingestion patterns
- [Transformation Layer](../02-Transformation/README.md) - Delta table transformations
- [PySpark Tutorials](../Pyspark/README.md) - DataFrame operations

---

**📧 Questions or Issues?** Check the main project [README](../README.md) for support information.
