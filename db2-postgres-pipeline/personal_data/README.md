# Personal Data Streaming Pipeline

This pipeline extracts personal data information from DB2 and streams it to PostgreSQL using RabbitMQ as a message queue.

## Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Batch Processing**: Configurable batch sizes for both DB2 extraction and PostgreSQL insertion
- **Dead Letter Queue**: Failed messages are sent to a dead letter queue for later analysis
- **Duplicate Prevention**: Uses ON CONFLICT to handle duplicate records
- **Thread-Safe**: Uses locks for statistics tracking across threads
- **Optimized SQL**: Two SQL versions available (v3 original, v4 optimized)

## SQL Versions

### v3 (Original)
- Complex location lookups with multiple fallback strategies
- Location lookup calculations performed for each row
- More comprehensive but slower due to repeated calculations

### v4 (Optimized - Default)
- **Same exact logic and output as v3**
- Pre-computes location lookups in CTEs to run once instead of per row
- Moves expensive location matching operations to WITH clauses
- **No changes to business logic** - only performance optimization through query reorganization
- Faster execution while maintaining identical data quality and results

## Usage

### Run with default settings (v4 SQL, optimized)
```bash
python run_personal_data_pipeline.py
```

### Run with original SQL (v3)
```bash
SQL_VERSION=v3 python run_personal_data_pipeline.py
```

### Clear queues before running
```bash
python clear_personal_data_queue.py
```

## Configuration

The pipeline uses the main `config.py` file for:
- DB2 connection settings
- PostgreSQL connection settings  
- RabbitMQ connection settings

## Performance Tuning

### Batch Sizes
- `batch_size`: Records fetched from DB2 per batch (default: 1000)
- `consumer_batch_size`: Records inserted to PostgreSQL per batch (default: 100)

### Environment Variables
- `SQL_VERSION`: Choose between 'v3' (original) or 'v4' (optimized, default)

## Queue Names
- Main queue: `personal_data_queue`
- Dead letter queue: `personal_data_dead_letter`

## Database Schema

The pipeline inserts data into the `personal_data_information` table with the following key fields:
- `customeridentificationnumber` (unique key)
- `fullnames`
- `gender`
- `nationality`
- `region`, `district`, `ward` (location fields)
- Contact information (mobile, email, etc.)
- Identification details

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

- `personal_data_streaming_pipeline.py`: Main pipeline implementation
- `run_personal_data_pipeline.py`: Runner script
- `clear_personal_data_queue.py`: Queue management utility
- `README.md`: This documentation

## Dependencies

- `pika`: RabbitMQ client
- `psycopg2`: PostgreSQL client
- `pyodbc`: DB2 client (via db2_connection module)