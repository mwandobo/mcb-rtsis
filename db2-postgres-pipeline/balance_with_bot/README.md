# Balance with BOT Streaming Pipeline

Streaming data pipeline for balance with Bank of Tanzania (BOT) data from DB2 to PostgreSQL using RabbitMQ message queue.

## Overview

This pipeline extracts balance with BOT data from DB2 and loads it into PostgreSQL using a streaming architecture with producer/consumer threads and RabbitMQ for message queuing.

## Source Query

- SQL File: `sqls/balances-bot-v1.sql`
- Source Tables: GLI_TRX_EXTRACT, GLG_ACCOUNT, CUSTOMER, CURRENCY, PROFITS_ACCOUNT, fixing_rate
- Target Table: `balanceWithBot`

## Data Fields (13 fields)

1. reportingDate
2. accountNumber
3. accountName
4. accountType
5. subAccountType
6. currency
7. orgAmount
8. usdAmount
9. tzsAmount
10. transactionDate
11. maturityDate
12. allowanceProbableLoss
13. botProvision

## Architecture

- **Producer Thread**: Executes DB2 query once and streams results via fetchmany()
- **Consumer Thread**: Processes messages from RabbitMQ queue with batch inserts
- **Message Queue**: RabbitMQ with dead-letter exchange for failed messages
- **Batch Processing**: 1000 records per producer batch, 100 records per consumer batch
- **Error Handling**: Retry logic with dead-letter queue for failed messages

## Files

- `balance_with_bot_streaming_pipeline.py` - Main streaming pipeline
- `create_balance_with_bot_table.py` - Creates PostgreSQL table
- `run_balance_with_bot_pipeline.py` - Runs complete pipeline
- `clear_balance_with_bot_queue.py` - Clears RabbitMQ queue

## Usage

### Run Complete Pipeline

```bash
python run_balance_with_bot_pipeline.py
```

### Run Individual Components

```bash
# Create table only
python create_balance_with_bot_table.py

# Run streaming pipeline only
python balance_with_bot_streaming_pipeline.py

# Clear RabbitMQ queue
python clear_balance_with_bot_queue.py
```

### Pipeline Modes

```bash
# Full streaming mode (default)
python balance_with_bot_streaming_pipeline.py --mode streaming

# Producer only
python balance_with_bot_streaming_pipeline.py --mode producer

# Consumer only
python balance_with_bot_streaming_pipeline.py --mode consumer
```

### Custom Batch Sizes

```bash
python balance_with_bot_streaming_pipeline.py --batch-size 2000 --consumer-batch-size 200
```

## Features

- Streaming architecture with simultaneous producer/consumer
- RabbitMQ message queuing with dead-letter exchange
- Batch processing for optimal performance
- Retry logic for connection failures
- Progress tracking with ETA
- Thread-safe statistics
- Persistent PostgreSQL connection for consumer

## Monitoring

The pipeline provides:
- Real-time progress updates
- Processing rate (records/second)
- ETA for completion
- Success rate statistics
- Detailed error logging

## Error Handling

- Failed messages are sent to dead-letter queue
- Automatic retry on connection failures
- PostgreSQL connection recovery
- RabbitMQ connection recovery
