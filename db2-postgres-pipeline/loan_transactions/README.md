# Loan Transactions Streaming Pipeline

This pipeline transfers loan transaction data from DB2 to PostgreSQL using a streaming architecture with RabbitMQ as the message queue.

## Architecture

- **Producer**: Fetches loan transaction records from DB2 using `loan-transaction.sql` and publishes to RabbitMQ
- **Consumer**: Reads from RabbitMQ and batch inserts into PostgreSQL
- **Message Queue**: RabbitMQ with dead-letter queue for failed messages
- **Database**: PostgreSQL table `loanTransactions`

## Files

- `loan_transactions_streaming_pipeline.py` - Main streaming pipeline (producer + consumer)
- `create_loan_transactions_table.py` - Creates the loanTransactions table in PostgreSQL
- `run_loan_transactions_pipeline.py` - Runner script to execute the pipeline
- `check_table.py` - Check table status and statistics
- `drop_table.py` - Drop the loanTransactions table
- `clear_loan_transactions_queue.py` - Clear RabbitMQ queue

## Setup

### 1. Create the PostgreSQL Table

```bash
python create_loan_transactions_table.py
```

This creates the `loanTransactions` table with all necessary columns and indexes.

### 2. Run the Pipeline

```bash
python run_loan_transactions_pipeline.py
```

Or run the streaming pipeline directly:

```bash
python loan_transactions_streaming_pipeline.py
```

## Table Structure

The `loanTransactions` table includes:

- Reporting date and transaction date
- Loan number
- Transaction type (Installment payment, Loan disbursement, Loan payoff, etc.)
- Transaction sub-type (New disbursement, Restructuring, Enhancement)
- Currency information
- Multi-currency amounts (original, USD, TZS)
- Timestamps (created_at, updated_at)

## Transaction Types

The pipeline categorizes transactions into:

- **Installment payment**: Regular loan payments
- **Loan disbursement**: New loan disbursements
- **Loan payoff**: Full loan payments
- **Reversal**: Transaction reversals
- **Prepayments**: Early payments
- **Interest Accruals**: Interest calculations
- **Deposit**: Deposit transactions
- **Loan administration fees**: Administrative charges
- **Interest capitalization**: Capitalized interest
- **Withdrawal**: Withdrawal transactions

## Features

- **Streaming Architecture**: Producer and consumer run simultaneously
- **Batch Processing**: Efficient batch inserts to PostgreSQL
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
- Statistics by transaction type
- Statistics by currency

### Clear Queue

```bash
python clear_loan_transactions_queue.py
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
python loan_transactions_streaming_pipeline.py --batch-size 2000 --consumer-batch-size 200
```

## Error Handling

- Failed messages are routed to `loan_transactions_dead_letter` queue
- PostgreSQL connection errors trigger automatic reconnection
- RabbitMQ connection errors trigger automatic retry
- All errors are logged with full context

## Data Source

Source SQL: `sqls/loan-transaction.sql`

The query filters transactions from specific GL accounts and includes:
- Transaction details from GLI_TRX_EXTRACT
- Loan account information
- GL account filtering
- Currency conversions (USD to TZS at 2500 rate)
