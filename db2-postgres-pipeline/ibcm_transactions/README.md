# IBCM Transactions Streaming Pipeline

This pipeline extracts Inter-Bank Call Money (IBCM) transaction data from DB2 and streams it to PostgreSQL using RabbitMQ as a message queue.

## Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Batch Processing**: Configurable batch sizes for both DB2 extraction and PostgreSQL insertion
- **Dead Letter Queue**: Failed messages are sent to a dead letter queue for later analysis
- **Duplicate Prevention**: Uses ON CONFLICT to handle duplicate records based on composite key
- **Thread-Safe**: Uses locks for statistics tracking across threads

## Setup

### Create the PostgreSQL table (first time only)
```bash
python create_table.py
```

### Run the pipeline
```bash
python run_ibcm_transactions_pipeline.py
```

### Clear queues before running
```bash
python clear_ibcm_transactions_queue.py
```

Note: The pipeline will automatically create the table if it doesn't exist, but you can also create it manually using the script above.

## Configuration

The pipeline uses the main `config.py` file for:
- DB2 connection settings
- PostgreSQL connection settings  
- RabbitMQ connection settings

## Performance Tuning

### Batch Sizes
- `batch_size`: Records fetched from DB2 per batch (default: 1000)
- `consumer_batch_size`: Records inserted to PostgreSQL per batch (default: 100)

## Queue Names
- Main queue: `ibcm_transactions_queue`
- Dead letter queue: `ibcm_transactions_dead_letter`

## Database Schema

The pipeline inserts data into the `ibcmTransactions` table with the following fields:
- `reportingDate`: Date and time of reporting
- `transactionDate`: Date and time of transaction (part of unique key)
- `lenderName`: Name of the lending institution (part of unique key)
- `borrowerName`: Name of the borrowing institution (part of unique key)
- `transactionType`: Type of transaction (Market/Off Market)
- `tzsAmount`: Transaction amount in TZS
- `tenure`: Duration of the transaction in days
- `interestRate`: Interest rate applied

## Unique Key

The pipeline uses a composite unique key on:
- `transactionDate`
- `lenderName` 
- `borrowerName`

This prevents duplicate transactions while allowing updates to existing records.

## Error Handling

- Invalid records are logged and skipped
- Database connection failures trigger retries
- RabbitMQ connection issues are handled gracefully
- Failed messages go to dead letter queue

## Monitoring

The pipeline provides real-time statistics:
- Total records produced
- Total records consumed
- Processing rate (records/second)
- Elapsed time

## Files

- `ibcm_transactions_streaming_pipeline.py`: Main pipeline implementation
- `run_ibcm_transactions_pipeline.py`: Runner script
- `clear_ibcm_transactions_queue.py`: Queue management utility
- `create_table.py`: PostgreSQL table creation script
- `create_ibcm_transactions_table.sql`: SQL DDL for table creation
- `README.md`: This documentation

## Dependencies

- `pika`: RabbitMQ client
- `psycopg2`: PostgreSQL client
- `pyodbc`: DB2 client (via db2_connection module)

## Data Source

The pipeline extracts data from:
- `GLI_TRX_EXTRACT`: Main transaction extract table
- `TREASURY_MM_DEAL`: Treasury money market deal details
- `SSI_PARTY`: Bank information for lender/borrower identification
- `PRODUCT`: Product descriptions for transaction classification