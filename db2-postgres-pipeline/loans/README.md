# Loans Streaming Pipeline

This pipeline transfers loan information data from DB2 to PostgreSQL using a streaming architecture with RabbitMQ as the message queue.

## Architecture

- **Producer**: Fetches loan records from DB2 using `loan-information-v6.sql` and publishes to RabbitMQ
- **Consumer**: Reads from RabbitMQ and batch inserts into PostgreSQL
- **Message Queue**: RabbitMQ with dead-letter queue for failed messages
- **Database**: PostgreSQL table `loanInformation`

## Files

- `loans_streaming_pipeline.py` - Main streaming pipeline (producer + consumer)
- `create_loans_table.py` - Creates the loanInformation table in PostgreSQL
- `run_loans_pipeline.py` - Runner script to execute the pipeline
- `check_table.py` - Check table status and statistics
- `drop_table.py` - Drop the loanInformation table
- `clear_loans_queue.py` - Clear RabbitMQ queue

## Setup

### 1. Create the PostgreSQL Table

```bash
python create_loans_table.py
```

This creates the `loanInformation` table with all necessary columns and indexes.

### 2. Run the Pipeline

```bash
python run_loans_pipeline.py
```

Or run the streaming pipeline directly:

```bash
python loans_streaming_pipeline.py
```

## Table Structure

The `loanInformation` table includes:

- Customer information (ID, name, country, gender, etc.)
- Loan details (number, type, phase, economic activity)
- Financial amounts (sanctioned, disbursed, outstanding, installment)
- Multi-currency support (original, USD, TZS)
- Collateral information (JSON format)
- Officer and supervisor information
- Timestamps (created_at, updated_at)

## Features

- **Streaming Architecture**: Producer and consumer run simultaneously
- **Batch Processing**: Efficient batch inserts to PostgreSQL
- **Duplicate Prevention**: ON CONFLICT handling using unique loanNumber
- **Error Handling**: Dead-letter queue for failed messages
- **Progress Tracking**: Real-time progress reports every 5 minutes
- **Retry Logic**: Automatic retry for transient failures
- **Connection Management**: Persistent PostgreSQL connection for better performance

## Monitoring

### Check Table Status

```bash
python check_table.py
```

Shows:
- Total record count
- Sample records
- Statistics by loan type
- Statistics by currency

### Clear Queue

```bash
python clear_loans_queue.py
```

Purges all messages from the RabbitMQ queue.

## Configuration

The pipeline uses configuration from the parent `config.py`:

- DB2 connection settings
- PostgreSQL connection settings
- RabbitMQ connection settings

## Performance

- Default batch size: 1000 records per DB2 fetch
- Default consumer batch size: 100 records per PostgreSQL insert
- Adjustable via command-line arguments:

```bash
python loans_streaming_pipeline.py --batch-size 2000 --consumer-batch-size 200
```

## Error Handling

- Failed messages are routed to `loans_dead_letter` queue
- PostgreSQL connection errors trigger automatic reconnection
- RabbitMQ connection errors trigger automatic retry
- All errors are logged with full context

## Data Source

Source SQL: `sqls/loan-information-v6.sql`

The query includes:
- Loan account information
- Customer details
- Collateral aggregation
- Currency conversions
- Officer assignments
- Installment details
