# Balance with Other Banks Streaming Pipeline

Streaming data pipeline for balance with other banks data from DB2 to PostgreSQL using RabbitMQ message queue.

## Overview

This pipeline extracts balance with other banks data from DB2 and loads it into PostgreSQL using a streaming architecture with producer/consumer threads and RabbitMQ for message queuing.

## Source Query

- SQL File: `sqls/balance-with-other-bank-v1.sql`
- Source Tables: GLI_TRX_EXTRACT, GLG_ACCOUNT, CUSTOMER, PROFITS_ACCOUNT, CURRENCY, fixing_rate
- Target Table: `balanceWithOtherBanks`

## Data Fields (21 fields)

1. reportingDate
2. accountNumber
3. accountName
4. bankCode
5. country
6. relationshipType
7. accountType
8. subAccountType
9. currency
10. orgAmount
11. usdAmount
12. tzsAmount
13. transactionDate
14. pastDueDays
15. allowanceProbableLoss
16. botProvision
17. assetsClassificationCategory
18. contractDate
19. maturityDate
20. externalRatingCorrespondentBank
21. gradesUnratedBanks

## Architecture

- **Producer Thread**: Executes DB2 query once and streams results via fetchmany()
- **Consumer Thread**: Processes messages from RabbitMQ queue with batch inserts
- **Message Queue**: RabbitMQ with dead-letter exchange for failed messages
- **Batch Processing**: 1000 records per producer batch, 100 records per consumer batch
- **Error Handling**: Retry logic with dead-letter queue for failed messages

## Files

- `balance_with_other_banks_streaming_pipeline.py` - Main streaming pipeline
- `create_balance_with_other_banks_table.py` - Creates PostgreSQL table
- `run_balance_with_other_banks_pipeline.py` - Runs complete pipeline
- `clear_balance_with_other_banks_queue.py` - Clears RabbitMQ queue

## Usage

### Run Complete Pipeline

```bash
python run_balance_with_other_banks_pipeline.py
```

### Run Individual Components

```bash
# Create table only
python create_balance_with_other_banks_table.py

# Run streaming pipeline only
python balance_with_other_banks_streaming_pipeline.py

# Clear RabbitMQ queue
python clear_balance_with_other_banks_queue.py
```

### Pipeline Modes

```bash
# Full streaming mode (default)
python balance_with_other_banks_streaming_pipeline.py --mode streaming

# Producer only
python balance_with_other_banks_streaming_pipeline.py --mode producer

# Consumer only
python balance_with_other_banks_streaming_pipeline.py --mode consumer
```

### Custom Batch Sizes

```bash
python balance_with_other_banks_streaming_pipeline.py --batch-size 2000 --consumer-batch-size 200
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
