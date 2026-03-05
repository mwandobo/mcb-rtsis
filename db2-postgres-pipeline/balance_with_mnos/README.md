# Balance with MNOs Streaming Pipeline

This pipeline extracts balance with MNO (Mobile Network Operator) data from DB2 and loads it into PostgreSQL using a streaming architecture with RabbitMQ as the message broker.

## Architecture

The pipeline uses a producer-consumer pattern:
- **Producer**: Reads balance with MNO data from DB2 using `balances-with-mnos.sql` and publishes to RabbitMQ queue
- **Consumer**: Consumes messages from RabbitMQ and batch inserts into PostgreSQL
- Both run simultaneously for optimal throughput

## Files

- `balance_with_mnos_streaming_pipeline.py` - Main pipeline with producer and consumer logic
- `create_balance_with_mnos_table.py` - Creates the PostgreSQL table schema
- `run_balance_with_mnos_pipeline.py` - Simple script to run the pipeline
- `clear_balance_with_mnos_queue.py` - Utility to clear RabbitMQ queue

## Setup

1. Create the PostgreSQL table:
```bash
python create_balance_with_mnos_table.py
```

2. Run the streaming pipeline:
```bash
python run_balance_with_mnos_pipeline.py
```

## Configuration

The pipeline uses settings from the parent `config.py`:
- DB2 connection settings
- PostgreSQL connection settings
- RabbitMQ connection settings

## Features

- **Streaming Architecture**: Producer and consumer run simultaneously
- **Batch Processing**: Efficient batch inserts to PostgreSQL (100 records per batch)
- **Dead Letter Queue**: Failed messages are routed to dead-letter queue for analysis
- **Retry Logic**: Automatic retry on connection failures
- **Progress Tracking**: Real-time progress reporting with ETA
- **Thread-Safe**: Uses locks for shared statistics

## Data Flow

1. Producer executes `balances-with-mnos.sql` query once and streams results via `fetchmany()`
2. Each record is validated and published to `balance_with_mnos_queue` in RabbitMQ
3. Consumer reads messages in batches and performs batch inserts to PostgreSQL
4. Failed messages are sent to `balance_with_mnos_dead_letter` queue

## Table Schema

The `balanceWithMnos` table contains 10 fields:
- reportingDate
- floatBalanceDate
- mnoCode
- tillNumber
- currency
- allowanceProbableLoss
- botProvision
- orgFloatAmount
- usdFloatAmount
- tzsFloatAmount

## Monitoring

The pipeline logs:
- Total records available
- Records produced and consumed
- Processing rate (records/second)
- Progress percentage
- ETA for completion
- Success rate

## Utilities

Clear the RabbitMQ queue:
```bash
python clear_balance_with_mnos_queue.py
```

## Error Handling

- Connection failures trigger automatic retry (3 attempts)
- PostgreSQL connection errors cause message requeue
- Invalid records are sent to dead-letter queue
- All errors are logged with details
