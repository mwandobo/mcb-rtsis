# Channel Record Information Pipeline

This folder contains the streaming pipeline for channel record information data processing.

## Files

- `channel_record_information_streaming_pipeline.py` - Main streaming pipeline with producer and consumer threads
- `create_channel_record_information_table.py` - PostgreSQL table creation script
- `run_channel_record_information_pipeline.py` - Pipeline runner script
- `clear_channel_record_information_queue.py` - Utility to clear RabbitMQ queue
- `test_channel_record_information_pipeline.py` - Comprehensive test suite
- `verify_data_quality.py` - Data quality verification tool

## Usage

### 1. Create the PostgreSQL table
```bash
python create_channel_record_information_table.py
```

### 2. Run the test suite
```bash
python test_channel_record_information_pipeline.py
```

### 3. Run the streaming pipeline
```bash
python run_channel_record_information_pipeline.py
```

### 4. Verify data quality
```bash
python verify_data_quality.py
```

### 5. Clear the queue (if needed)
```bash
python clear_channel_record_information_queue.py
```

## Pipeline Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Batch Processing**: Configurable batch sizes for both DB2 queries and PostgreSQL inserts
- **Error Handling**: Retry logic with dead-letter queues for failed messages
- **Duplicate Prevention**: Unique index handling with ON CONFLICT processing
- **Progress Monitoring**: Real-time progress reports and statistics
- **Connection Management**: Persistent connections with automatic reconnection

## Configuration

The pipeline uses the same configuration as other pipelines in the system:
- DB2 connection settings
- PostgreSQL connection settings  
- RabbitMQ connection settings

## Data Source

The pipeline processes channel record information data from the database, extracting information about:
- Channel types and categories
- Transaction channels and methods
- Channel performance metrics
- Channel availability and status
- Channel configuration details

## Table Structure

The target PostgreSQL table contains fields for:
- Channel identification and classification
- Channel operational details
- Performance and availability metrics
- Configuration and status information
- Audit fields (created_at, updated_at)

## Performance Considerations

- Uses streaming architecture for large datasets
- Implements batch processing for efficient database operations
- Includes connection pooling and retry mechanisms
- Provides real-time progress monitoring and ETA calculations