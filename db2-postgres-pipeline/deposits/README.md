# Deposits Streaming Pipeline

Streaming pipeline for deposits data from DB2 to PostgreSQL using RabbitMQ.

## Files

- `deposits_streaming_pipeline.py` - Main pipeline with producer and consumer threads
- `create_deposits_table.py` - Create PostgreSQL table
- `run_deposits_pipeline.py` - Run the pipeline
- `check_table.py` - Check table contents
- `drop_table.py` - Drop the table
- `clear_deposits_queue.py` - Clear RabbitMQ queue

## Usage

1. Create table:
```bash
python create_deposits_table.py
```

2. Run pipeline:
```bash
python run_deposits_pipeline.py
```

3. Check results:
```bash
python check_table.py
```

## Data Structure

Based on `deposits-v1.sql` with 37 columns including:
- Transaction details (unique ref, timestamp, amount)
- Account information (number, name, type)
- Customer details (ID, category, country)
- Location data (region, district)
- Currency conversions (original, USD, TZS)
- Interest information

## Features

- Streaming architecture (producer + consumer run simultaneously)
- Batch processing for efficiency
- Automatic retry on failures
- Dead-letter queue for failed messages
- Duplicate prevention via unique transaction reference
- Progress tracking and statistics
