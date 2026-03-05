# Agents Streaming Pipeline

This pipeline extracts agent data from DB2 and loads it into PostgreSQL using a streaming architecture with RabbitMQ as the message broker.

## Architecture

The pipeline uses a producer-consumer pattern:
- **Producer**: Reads agent data from DB2 using `agents-from-agents-list-NEW-V5.table.sql` and publishes to RabbitMQ queue
- **Consumer**: Consumes messages from RabbitMQ and batch inserts into PostgreSQL
- Both run simultaneously for optimal throughput

## Files

- `agents_streaming_pipeline.py` - Main pipeline with producer and consumer logic
- `create_agents_table.py` - Creates the PostgreSQL table schema
- `run_agents_pipeline.py` - Simple script to run the pipeline
- `clear_agents_queue.py` - Utility to clear RabbitMQ queue

## Setup

1. Create the PostgreSQL table:
```bash
python create_agents_table.py
```

2. Run the streaming pipeline:
```bash
python run_agents_pipeline.py
```

## Configuration

The pipeline uses settings from the parent `config.py`:
- DB2 connection settings
- PostgreSQL connection settings
- RabbitMQ connection settings

## Features

- **Streaming Architecture**: Producer and consumer run simultaneously
- **Batch Processing**: Efficient batch inserts to PostgreSQL (100 records per batch)
- **Duplicate Prevention**: Uses unique index on `agentId` with ON CONFLICT DO NOTHING
- **Dead Letter Queue**: Failed messages are routed to dead-letter queue for analysis
- **Retry Logic**: Automatic retry on connection failures
- **Progress Tracking**: Real-time progress reporting with ETA
- **Thread-Safe**: Uses locks for shared statistics

## Data Flow

1. Producer executes `agents-from-agents-list-NEW-V5.table.sql` query once and streams results via `fetchmany()`
2. Each record is validated and published to `agents_queue` in RabbitMQ
3. Consumer reads messages in batches and performs batch inserts to PostgreSQL
4. Duplicate records (same `agentId`) are automatically skipped
5. Failed messages are sent to `agents_dead_letter` queue

## Table Schema

The `agents` table contains 25 fields:
- reportingDate
- agentName
- agentId (unique)
- tillNumber
- businessForm
- agentPrincipal
- agentPrincipalName
- gender
- registrationDate
- closedDate
- certIncorporation
- nationality
- agentStatus
- agentType
- accountNumber
- region
- district
- ward
- street
- houseNumber
- postalCode
- country
- gpsCoordinates
- agentTaxIdentificationNumber
- businessLicense

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
python clear_agents_queue.py
```

## Error Handling

- Connection failures trigger automatic retry (3 attempts)
- PostgreSQL connection errors cause message requeue
- Invalid records are sent to dead-letter queue
- All errors are logged with details
