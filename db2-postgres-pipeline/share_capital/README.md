# Share Capital Pipeline

This folder contains the streaming pipeline for share capital data processing.

## Files

- `share_capital_streaming_pipeline.py` - Main streaming pipeline with producer and consumer threads
- `create_share_capital_table.py` - PostgreSQL table creation script
- `run_share_capital_pipeline.py` - Pipeline runner script
- `clear_share_capital_queue.py` - Utility to clear RabbitMQ queue

## Usage

### 1. Create the PostgreSQL table
```bash
python create_share_capital_table.py
```

### 2. Run the streaming pipeline
```bash
python run_share_capital_pipeline.py
```

### 3. Clear the queue (if needed)
```bash
python clear_share_capital_queue.py
```

## Pipeline Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Batch Processing**: Configurable batch sizes for both DB2 queries and PostgreSQL inserts
- **Error Handling**: Retry logic with dead-letter queues for failed messages
- **Duplicate Prevention**: Unique index on shareholderNames, transactionDate, capitalCategory with ON CONFLICT handling
- **Progress Monitoring**: Real-time progress reports and statistics
- **Connection Management**: Persistent connections with automatic reconnection

## Configuration

The pipeline uses the same configuration as other pipelines in the system:
- DB2 connection settings
- PostgreSQL connection settings  
- RabbitMQ connection settings

## Data Source

The pipeline processes data from the `share-capital.sql` query, which extracts data from:
- `SHARE_CAPITAL` table (main source for all share capital information)

## Table Structure

The target PostgreSQL table `shareCapital` contains 14 fields including:
- Reporting and transaction dates
- Capital category and subcategory
- Transaction type and shareholder information
- Client type and country
- Share details (number of shares, price/book value)
- Amount information in multiple currencies
- Sector SNA classification
- Audit fields (created_at, updated_at)

## Share Capital Information

The pipeline processes share capital data including:
- **Capital Categories**: Different types of capital (e.g., Ordinary Shares, Preference Shares)
- **Shareholder Details**: Names, types (individual/corporate), and countries
- **Transaction Information**: Dates, types (issue, redemption, transfer)
- **Financial Data**: Number of shares, prices, amounts in original and TZS currencies
- **Classification**: Sector SNA (System of National Accounts) classification