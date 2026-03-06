# Personal Data Streaming Pipeline

This pipeline transfers personal/customer data from DB2 to PostgreSQL using a streaming architecture with RabbitMQ as the message queue.

## Architecture

- **Producer**: Fetches personal data records from DB2 using `personal_data_information-v4.sql` and publishes to RabbitMQ
- **Consumer**: Reads from RabbitMQ and batch inserts into PostgreSQL
- **Message Queue**: RabbitMQ with dead-letter queue for failed messages
- **Database**: PostgreSQL table `personalDataInformation`

## Files

- `personal_data_streaming_pipeline.py` - Main streaming pipeline (producer + consumer)
- `create_personal_data_table.py` - Creates the personalDataInformation table in PostgreSQL
- `run_personal_data_pipeline.py` - Runner script to execute the pipeline
- `check_table.py` - Check table status and statistics
- `drop_table.py` - Drop the personalDataInformation table
- `clear_personal_data_queue.py` - Clear RabbitMQ queue

## Setup

### 1. Create the PostgreSQL Table

```bash
python create_personal_data_table.py
```

This creates the `personalDataInformation` table with all necessary columns and indexes.

### 2. Run the Pipeline

```bash
python run_personal_data_pipeline.py
```

Or run the streaming pipeline directly:

```bash
python personal_data_streaming_pipeline.py
```

## Table Structure

The `personalDataInformation` table includes 78 columns covering:

- **Personal Information**: Names, gender, marital status, birth details
- **Identification**: ID type, number, issuance and expiration dates
- **Contact Information**: Mobile, email, fax, social media
- **Address Information**: Region, district, ward, street, postal code
- **Employment**: Status, profession, employer details, monthly income
- **Education**: Education level, number of dependants
- **Business**: Business name, registration, license, TIN
- **Spouse Information**: Spouse name and identification
- **Location Details**: Birth and current address with region/district/ward mapping
- **Timestamps**: created_at, updated_at

## Features

- **Streaming Architecture**: Producer and consumer run simultaneously
- **Batch Processing**: Efficient batch inserts to PostgreSQL
- **Duplicate Prevention**: ON CONFLICT handling using unique customerIdentificationNumber
- **Error Handling**: Dead-letter queue for failed messages
- **Progress Tracking**: Real-time progress reports every 5 minutes
- **Retry Logic**: Automatic retry for transient failures
- **Connection Management**: Persistent PostgreSQL connection for better performance
- **Complex Location Mapping**: Uses bank_location_lookup_v2 for region/district/ward resolution

## Monitoring

### Check Table Status

```bash
python check_table.py
```

Shows:
- Total record count
- Sample records
- Statistics by gender
- Statistics by region

### Clear Queue

```bash
python clear_personal_data_queue.py
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
python personal_data_streaming_pipeline.py --batch-size 2000 --consumer-batch-size 200
```

## Error Handling

- Failed messages are routed to `personal_data_dead_letter` queue
- PostgreSQL connection errors trigger automatic reconnection
- RabbitMQ connection errors trigger automatic retry
- All errors are logged with full context

## Data Source

Source SQL: `sqls/personal_data_information-v4.sql`

The query includes:
- Customer personal information
- Address and location mapping using CTEs for performance
- Identification details with validation
- Employment and business information
- Contact information
- Complex joins with generic_detail, customer_category, and location lookups
- Filters for valid customer types and identification documents

## Data Quality

The pipeline filters out:
- Non-individual customers (only CUST_TYPE = '1')
- Invalid identification types (OTHER TYPE, BIRTH CERTIFICATE, N/A)
- Invalid National ID cards (less than 20 characters)
- Invalid Driving Licenses (less than 10 characters)
- Invalid Voter IDs (not starting with 'T')
