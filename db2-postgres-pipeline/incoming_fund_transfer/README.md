# Incoming Fund Transfer Pipeline

This folder contains the streaming pipeline for incoming fund transfer data processing.

## Files

- `incoming_fund_transfer_streaming_pipeline.py` - Main streaming pipeline with producer and consumer threads
- `create_incoming_fund_transfer_table.py` - PostgreSQL table creation script
- `run_incoming_fund_transfer_pipeline.py` - Pipeline runner script
- `clear_incoming_fund_transfer_queue.py` - Utility to clear RabbitMQ queue

## Usage

### 1. Create the PostgreSQL table
```bash
python create_incoming_fund_transfer_table.py
```

### 2. Run the streaming pipeline
```bash
python run_incoming_fund_transfer_pipeline.py
```

### 3. Clear the queue (if needed)
```bash
python clear_incoming_fund_transfer_queue.py
```

## Pipeline Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Batch Processing**: Configurable batch sizes for both DB2 queries and PostgreSQL inserts
- **Error Handling**: Retry logic with dead-letter queues for failed messages
- **Duplicate Prevention**: Unique index on transactionId with ON CONFLICT handling
- **Progress Monitoring**: Real-time progress reports and statistics
- **Connection Management**: Persistent connections with automatic reconnection

## Configuration

The pipeline uses the same configuration as other pipelines in the system:
- DB2 connection settings
- PostgreSQL connection settings  
- RabbitMQ connection settings

## Data Source

The pipeline processes data from the `incoming-fund-transfer.sql` query, which extracts data from:
- `IPS_MESSAGE_HEADER` (main table)
- `CUSTOMER` (customer information)
- `OTHER_ID` (identification details)
- `CURRENCY` (currency information)
- `FIXING_RATE` (exchange rates)

## Table Structure

The target PostgreSQL table `incomingFundTransfer` contains 22 fields including:
- Transaction identification and dates
- Recipient and sender information
- Transfer channel and service details
- Amount information in multiple currencies
- Audit fields (created_at, updated_at)