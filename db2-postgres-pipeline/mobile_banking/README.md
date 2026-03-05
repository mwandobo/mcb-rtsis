# Mobile Banking Pipeline

This folder contains the streaming pipeline for mobile banking transaction data processing.

## Files

- `mobile_banking_streaming_pipeline.py` - Main streaming pipeline with producer and consumer threads
- `create_mobile_banking_table.py` - PostgreSQL table creation script
- `run_mobile_banking_pipeline.py` - Pipeline runner script
- `clear_mobile_banking_queue.py` - Utility to clear RabbitMQ queue

## Usage

### 1. Create the PostgreSQL table
```bash
python create_mobile_banking_table.py
```

### 2. Run the streaming pipeline
```bash
python run_mobile_banking_pipeline.py
```

### 3. Clear the queue (if needed)
```bash
python clear_mobile_banking_queue.py
```

## Pipeline Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Batch Processing**: Configurable batch sizes for both DB2 queries and PostgreSQL inserts
- **Error Handling**: Retry logic with dead-letter queues for failed messages
- **Duplicate Prevention**: Unique index on transactionRef with ON CONFLICT handling
- **Progress Monitoring**: Real-time progress reports and statistics
- **Connection Management**: Persistent connections with automatic reconnection

## Configuration

The pipeline uses the same configuration as other pipelines in the system:
- DB2 connection settings
- PostgreSQL connection settings  
- RabbitMQ connection settings

## Data Source

The pipeline processes data from the `mobile-banking-v1.sql` query, which extracts data from:
- `GLI_TRX_EXTRACT` (main transaction table)
- `PROFITS_ACCOUNT` (account information)
- `W_DIM_CUSTOMER` (customer details)
- `CURRENCY` (currency information)
- `FIXING_RATE` (exchange rates for proper currency conversion)

## Table Structure

The target PostgreSQL table `mobileBanking` contains 18 fields including:
- Transaction identification and dates
- Account and customer information
- Mobile transaction type (Deposit, Withdraw, Payment)
- Service category and status
- Amount information in multiple currencies
- Tax and levy amounts
- Audit fields (created_at, updated_at)

## Transaction Types

The pipeline processes three types of mobile banking transactions:
- **Deposit**: Funds deposited via mobile banking
- **Withdraw**: Funds withdrawn via mobile banking
- **Payment**: Payments made through mobile banking