# Personal Data Streaming Pipeline

## Overview

This pipeline streams personal data from DB2 to PostgreSQL using RabbitMQ as a message queue. It's based on the `personal_data_information-v3.sql` query and follows the same pattern as the agents and POS streaming pipelines.

## Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Cursor-based Pagination**: Efficiently processes large datasets using customer ID as cursor
- **Location Mapping**: Complex location lookups with fallback logic for region, district, and ward
- **Data Validation**: Skips records with missing required fields (region, ward)
- **Retry Logic**: Automatic retry for DB2, RabbitMQ, and PostgreSQL operations
- **Progress Tracking**: Real-time progress reports every 5 minutes
- **Conflict Handling**: ON CONFLICT DO UPDATE for idempotent inserts

## Architecture

```
DB2 (customer table) 
    ↓ (Producer Thread)
RabbitMQ Queue (personal_data_queue)
    ↓ (Consumer Thread)
PostgreSQL (personalData table)
```

## Key Components

### 1. PersonalDataRecord Dataclass
Contains 75+ fields including:
- Customer identification
- Personal information (name, gender, marital status)
- Location data (birth location, current address)
- Identification documents
- Employment and financial information
- Contact details

### 2. Location Mapping
Complex location resolution with multiple fallback strategies:
- Birth location: birthplace → district → ward
- Current location: city → region → district → ward
- Uses `bank_location_lookup_v2` table for standardization

### 3. Data Filtering
WHERE clause filters:
- Only personal customers (CUST_TYPE = '1')
- Valid identification types (excludes 'OTHER TYPE OF IDENTIFICATION', 'BIRTH CERTIFICATE', 'N/A')
- Valid ID lengths:
  - National ID: must be 20 characters
  - Driving License: must be at least 10 characters
  - Voters ID: must start with 'T'

## Usage

### Basic Usage
```bash
python run_personal_data_streaming_pipeline.py
```

### With Custom Batch Size
```bash
python personal_data_streaming_pipeline.py --batch-size 1000
```

### Direct Import
```python
from personal_data_streaming_pipeline import PersonalDataStreamingPipeline

pipeline = PersonalDataStreamingPipeline(batch_size=500)
pipeline.run_streaming_pipeline()
```

## Configuration

The pipeline uses the `Config` class from `config.py`:

```python
# Database connections
pg_host, pg_port, pg_database, pg_user, pg_password

# RabbitMQ settings
rabbitmq_host, rabbitmq_port, rabbitmq_user, rabbitmq_password
```

## Performance

- **Batch Size**: 500 records per batch (configurable)
- **Prefetch Count**: 10 messages for consumer
- **Retry Attempts**: 3 retries with 2-second delay
- **Progress Reports**: Every 5 minutes
- **Consumer Logging**: Every 100 records

## Database Schema

### Target Table: personalData

Key fields:
- `customerIdentificationNumber` (PRIMARY KEY)
- `reportingDate`, `firstName`, `middleNames`, `otherNames`
- `fullNames`, `gender`, `maritalStatus`
- `identificationType`, `identificationNumber`
- `region`, `district`, `ward` (location fields)
- `mobileNumber`, `emailAddress`
- And 60+ more fields...

### Indexes
- `idx_personalData_customerIdentificationNumber`
- `idx_personalData_reportingDate`
- `idx_personalData_identificationNumber`

## Error Handling

### Record Skipping
Records are skipped if:
- Region is NULL or empty
- Ward is NULL or empty

### Retry Logic
- DB2 queries: 3 retries
- RabbitMQ publish: 3 retries with reconnection
- PostgreSQL insert: 3 retries

### Graceful Shutdown
- Handles Ctrl+C (KeyboardInterrupt)
- Waits for consumer to finish processing queue
- Provides final statistics

## Monitoring

### Log Output
```
2024-01-15 10:30:00 - INFO - Producer: Batch 1 - 500 records, 485 published (2.5% complete, 12.3s)
2024-01-15 10:30:15 - INFO - Consumer: Processed 100 records (0.5%) - Rate: 6.7 rec/sec
2024-01-15 10:35:00 - INFO - PROGRESS REPORT: 2,500/100,000 records (2.5%) - Rate: 8.3 rec/sec - ETA: 195.8 minutes
```

### Final Summary
```
==========================================
Personal Data Pipeline Summary:
==========================================
Total available records: 100,000
Records produced: 98,500
Records consumed: 98,500
Success rate: 100.0%
Total processing time: 180.5 minutes
Average rate: 9.1 records/second
==========================================
```

## Comparison with Other Pipelines

| Feature | Agents | POS | Personal Data |
|---------|--------|-----|---------------|
| Source Table | AGENTS_LIST_V4 | AGENT_TERMINAL | customer |
| Queue Name | agents_queue | pos_queue | personal_data_queue |
| Cursor Field | AGENT_ID | FK_USRCODE | cust_id |
| Required Fields | agentTaxId, businessLicense | region, district, ward | region, ward |
| Complexity | Medium | Low | High |
| Location Mapping | Yes | Yes | Yes (extensive) |

## Troubleshooting

### Issue: No records produced
- Check WHERE clause filters
- Verify customer table has CUST_TYPE = '1' records
- Check identification type filters

### Issue: Records skipped
- Check logs for validation failures
- Verify region and ward are populated in source data
- Review location mapping logic

### Issue: Slow performance
- Increase batch size: `--batch-size 1000`
- Check DB2 connection performance
- Monitor RabbitMQ queue depth
- Verify PostgreSQL indexes exist

### Issue: Connection errors
- Verify DB2 connection in `config.py`
- Check RabbitMQ is running and accessible
- Verify PostgreSQL connection settings

## Related Files

- `sqls/personal_data_information-v3.sql` - Source SQL query
- `create_personal_data_table.py` - Table creation script
- `agents_streaming_pipeline.py` - Reference implementation
- `pos_streaming_pipeline.py` - Reference implementation
- `config.py` - Configuration settings
- `db2_connection.py` - DB2 connection manager

## Notes

- The pipeline uses the v3 SQL query with extensive location mapping
- Location fallback logic ensures most records get valid region/district/ward values
- The cursor-based pagination ensures no records are missed or duplicated
- The streaming architecture allows for processing very large datasets efficiently
