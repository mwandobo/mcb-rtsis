# Outgoing Fund Transfer Pipeline

This folder contains the streaming pipeline for outgoing fund transfer data processing.

## Files

- `outgoing_fund_transfer_streaming_pipeline.py` - Main streaming pipeline with producer and consumer threads
- `create_outgoing_fund_transfer_table.py` - PostgreSQL table creation script
- `run_outgoing_fund_transfer_pipeline.py` - Pipeline runner script
- `clear_outgoing_fund_transfer_queue.py` - Utility to clear RabbitMQ queue

## Usage

### 1. Create the PostgreSQL table
```bash
python create_outgoing_fund_transfer_table.py
```

### 2. Run the streaming pipeline
```bash
python run_outgoing_fund_transfer_pipeline.py
```

### 3. Clear the queue (if needed)
```bash
python clear_outgoing_fund_transfer_queue.py
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

The pipeline processes data from the `outgoing-fund-transfer-v1.sql` query, which extracts data from:
- `OUTGOING_ORDERS` (main table)
- `FIXING_RATE` (exchange rates with proper currency conversion logic)

## Table Structure

The target PostgreSQL table `outgoingFundTransfer` contains 25 fields including:
- Transaction identification and dates
- Sender and recipient information
- Transfer channel and service details
- Amount information in multiple currencies
- Transaction place and instructions
- Audit fields (created_at, updated_at)

## Key Differences from Incoming Fund Transfer

- Uses `OUTGOING_ORDERS` table as primary data source
- Includes additional fields like `recipientMobileNumber` and `transactionPlace`
- Different field mappings based on outgoing transfer structure
- Sender and recipient roles are reversed compared to incoming transfers