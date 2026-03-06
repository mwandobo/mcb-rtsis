# Personal Data Corporates Streaming Pipeline

Transfers corporate customer data from DB2 to PostgreSQL using streaming architecture with RabbitMQ.

## Architecture

- **Producer**: Fetches corporate data from DB2 using `personal-data-corporates-v4.sql`
- **Consumer**: Batch inserts into PostgreSQL
- **Queue**: RabbitMQ with dead-letter queue
- **Table**: `personalDataCorporates`

## Files

- `personal_data_corporates_streaming_pipeline.py` - Main pipeline
- `create_personal_data_corporates_table.py` - Table creation
- `run_personal_data_corporates_pipeline.py` - Runner script
- `check_table.py` - Check status
- `drop_table.py` - Drop table
- `clear_personal_data_corporates_queue.py` - Clear queue

## Setup

### 1. Create Table

```bash
python create_personal_data_corporates_table.py
```

### 2. Run Pipeline

```bash
python run_personal_data_corporates_pipeline.py
```

## Table Structure (52 columns)

- **Company Info**: Name, established date, legal form, employees
- **Registration**: Country, number, TIN, trade name
- **Parent/Group**: Parent name, incorporation number, group ID
- **Related Customers**: JSON array of directors/shareholders
- **Address**: Street, region, district, ward, postal code, PO Box
- **Secondary Address**: Alternative address fields
- **Contact**: Mobile, email, fax, social media
- **Entity Relations**: Entity details, share ownership
- **Timestamps**: created_at, updated_at

## Features

- Streaming architecture (simultaneous producer/consumer)
- Batch processing for efficiency
- Duplicate prevention via unique customerIdentificationNumber
- Dead-letter queue for failed messages
- Progress tracking every 5 minutes
- Automatic retry logic
- Complex location mapping with region/district/ward resolution
- Related customers stored as JSON

## Monitoring

```bash
python check_table.py  # Check status
python clear_personal_data_corporates_queue.py  # Clear queue
```

## Performance

- Default batch size: 1000 records
- Consumer batch size: 100 records
- Adjustable via command-line

## Data Source

Source: `sqls/personal-data-corporates-v4.sql`

Includes:
- Corporate customer information from W_DIM_CUSTOMER
- Agreement and address details
- Related individuals (directors/shareholders)
- Location mapping using bank_location_lookup_v2
- Complex CTEs for performance optimization
