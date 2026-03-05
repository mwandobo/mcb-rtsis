# Cash Information Pipeline

This folder contains the streaming pipeline for cash information data processing.

## Files

- `cash_streaming_pipeline.py` - Main streaming pipeline with producer and consumer threads
- `create_cash_information_table.py` - PostgreSQL table creation script
- `run_cash_information_pipeline.py` - Pipeline runner script
- `clear_cash_information_queue.py` - Utility to clear RabbitMQ queue

## Usage

### 1. Create the PostgreSQL table
```bash
python create_cash_information_table.py
```

### 2. Run the streaming pipeline
```bash
python run_cash_information_pipeline.py
```

### 3. Clear the queue (if needed)
```bash
python clear_cash_information_queue.py
```

## Pipeline Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Batch Processing**: Configurable batch sizes for both DB2 queries and PostgreSQL inserts
- **Error Handling**: Retry logic with dead-letter queues for failed messages
- **Duplicate Prevention**: Unique index on branchCode, transactionDate, cashCategory with ON CONFLICT handling
- **Progress Monitoring**: Real-time progress reports and statistics
- **Connection Management**: Persistent connections with automatic reconnection

## Configuration

The pipeline uses the same configuration as other pipelines in the system:
- DB2 connection settings
- PostgreSQL connection settings  
- RabbitMQ connection settings

## Data Source

The pipeline processes data from the `cash-information.sql` query, which extracts data from:
- `GLI_TRX_EXTRACT` (main transaction table)
- `CURRENCY` (currency information for proper conversion)
- `FIXING_RATE` (exchange rates with latest activation date/time logic)

The query uses `FK_GLG_ACCOUNTACCO` to filter cash-related accounts directly without joining to `GLG_ACCOUNT`.

## Table Structure

The target PostgreSQL table `cashInformation` contains 15 fields including:
- Reporting and transaction dates
- Branch code and cash category information
- Cash submission time and currency
- Amount information in multiple currencies (original, USD, TZS)
- Maturity date and provision fields
- Audit fields (created_at, updated_at)

## Cash Categories

The pipeline processes the following cash categories:
- **Cash in vault**: Clean notes stored in bank vaults
- **Petty cash**: Small amounts for daily operational expenses
- **Cash in ATMs**: Cash loaded in ATM machines
- **Cash with Tellers**: Cash held by bank tellers for transactions

## Cash Sub-Categories

- **CleanNotes**: For cash in vault
- **Notes**: For petty cash, ATM cash, and teller cash