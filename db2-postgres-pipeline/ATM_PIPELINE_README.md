# ATM Data Pipeline - BOT Project

## Overview
The ATM data pipeline extracts ATM information from DB2 and loads it into PostgreSQL using RabbitMQ for message queuing. This pipeline follows the same architecture as the existing cash and branch pipelines.

## Architecture
```
DB2 (AGENT_TERMINAL + GLI_TRX_EXTRACT) → RabbitMQ → PostgreSQL (atmInformation)
```

## Components

### 1. ATM Processor (`processors/atm_processor.py`)
- **AtmRecord**: Data class representing ATM information
- **AtmProcessor**: Handles data transformation and PostgreSQL insertion
- Validates ATM records before processing
- Supports currency conversion (TZS/USD)

### 2. Pipeline Scripts

#### Main Pipeline (`atm_pipeline_rabbitmq.py`)
- **Professional ATM Pipeline**: Full-featured pipeline with tracking and RabbitMQ
- Supports manual start timestamps
- Batch processing with configurable limits
- Automatic tracking to prevent duplicate processing
- Error handling and logging

#### Simple Pipeline (`simple_atm_pipeline.py`)
- **Simple ATM Pipeline**: Direct DB2 to PostgreSQL without RabbitMQ
- Good for testing and small data sets
- Synchronous processing

#### Consumer (`consume_atm_queue.py`)
- **ATM Queue Consumer**: Standalone consumer for RabbitMQ messages
- Can be run separately to process queued ATM records
- Handles message acknowledgment and error recovery

### 3. Database Schema

#### Source (DB2)
- **AGENT_TERMINAL**: ATM terminal information
- **GLI_TRX_EXTRACT**: Transaction data for cash loading/dispensing
- **GLG_ACCOUNT**: Account mapping for ATM cash accounts

#### Target (PostgreSQL)
```sql
CREATE TABLE "atmInformation" (
    "id" SERIAL PRIMARY KEY,
    "reportingDate" TIMESTAMP,
    "atmId" VARCHAR(50),
    "atmName" VARCHAR(200),
    "branchCode" VARCHAR(20),
    "atmLocation" VARCHAR(200),
    "region" VARCHAR(100),
    "district" VARCHAR(100),
    "ward" VARCHAR(100),
    "street" VARCHAR(200),
    "gpsCoordinates" VARCHAR(100),
    "atmType" VARCHAR(50),
    "atmStatus" VARCHAR(50),
    "installationDate" DATE,
    "lastMaintenanceDate" DATE,
    "currency" VARCHAR(10),
    "cashLoadedAmount" DECIMAL(15,2),
    "usdCashLoaded" DECIMAL(15,2),
    "tzsCashLoaded" DECIMAL(15,2),
    "cashDispensedAmount" DECIMAL(15,2),
    "usdCashDispensed" DECIMAL(15,2),
    "tzsCashDispensed" DECIMAL(15,2),
    "transactionCount" INTEGER,
    "transactionDate" DATE,
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2)
);
```

## Configuration

### ATM Table Configuration (config.py)
```python
'atm_information': TableConfig(
    name='atm_information',
    query="...",  # Complex query joining AGENT_TERMINAL and GLI_TRX_EXTRACT
    timestamp_column='transactionDate',
    target_table='atm_information',
    queue_name='atm_information_queue',
    processor_class='AtmProcessor',
    batch_size=1000,
    poll_interval=10
)
```

### Environment Variables (.env)
```
# Database connections
DB2_HOST=172.10.2.42
DB2_PORT=50000
DB2_DATABASE=mcbecho
DB2_USER=profits
DB2_PASSWORD=prft2016

PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=bankdb
PG_USER=postgres
PG_PASSWORD=postgres

# RabbitMQ
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
```

## Usage

### 1. Setup Database Schema
```bash
# Create PostgreSQL tables
psql -h localhost -U postgres -d bankdb -f sql/postgres-schema.sql
```

### 2. Run Professional Pipeline
```bash
# Full pipeline with RabbitMQ and tracking
python atm_pipeline_rabbitmq.py
```

### 3. Run Simple Pipeline (Testing)
```bash
# Direct processing without RabbitMQ
python simple_atm_pipeline.py
```

### 4. Run Consumer Only
```bash
# Process existing queue messages
python consume_atm_queue.py
```

## Data Flow

### 1. Data Extraction
- Queries `AGENT_TERMINAL` for ATM terminal information
- Joins with `GLI_TRX_EXTRACT` for transaction data
- Filters for ATM-related GL accounts (101000010, 101000015)
- Groups by terminal to aggregate transaction counts and amounts

### 2. Data Transformation
- Maps terminal types to ATM categories
- Derives location information (region, district, ward)
- Calculates cash loaded vs dispensed amounts
- Handles currency conversion (TZS/USD)
- Aggregates transaction counts per ATM

### 3. Data Loading
- Publishes to RabbitMQ queue (`atm_information_queue`)
- Consumer processes messages and inserts to PostgreSQL
- Tracks processing timestamps to prevent duplicates

## Key Features

### 1. Incremental Processing
- Uses `transactionDate` for tracking
- Prevents duplicate processing
- Supports manual start timestamps
- Configurable lookback periods

### 2. Error Handling
- Validates records before processing
- Logs detailed error information
- Continues processing on individual record failures
- Message acknowledgment in RabbitMQ

### 3. Monitoring
- Comprehensive logging with emojis for readability
- Processing statistics and timing
- Sample data display for verification
- Batch progress tracking

### 4. Scalability
- RabbitMQ for decoupled processing
- Configurable batch sizes
- Parallel consumer support
- Connection pooling

## ATM Data Fields

### Core Information
- **atmId**: Unique ATM identifier (ATM + TERMINAL_ID)
- **atmName**: ATM terminal name
- **branchCode**: Associated branch code
- **atmLocation**: Physical location description

### Location Details
- **region**: Derived from location (Dar es Salaam, Mwanza, etc.)
- **district**: Sub-region (Kinondoni, Ilala, etc.)
- **ward**: Local area (Msasani, Kariakoo, etc.)
- **street**: Street address
- **gpsCoordinates**: GPS coordinates (placeholder)

### Operational Data
- **atmType**: Cash Dispenser, Cash Deposit Machine, Multi-Function ATM
- **atmStatus**: Active, Inactive, Maintenance
- **installationDate**: When ATM was installed
- **lastMaintenanceDate**: Last maintenance date

### Financial Data
- **cashLoadedAmount**: Amount loaded into ATM
- **cashDispensedAmount**: Amount dispensed by ATM
- **transactionCount**: Number of transactions
- **currency**: Currency code (TZS, USD)
- Multi-currency support with TZS/USD amounts

## Troubleshooting

### Common Issues

1. **No ATM records found**
   - Check AGENT_TERMINAL table has data
   - Verify terminal types (ATM, CDM, MULTI)
   - Check date filters in query

2. **RabbitMQ connection errors**
   - Verify RabbitMQ is running
   - Check connection credentials
   - Ensure queue is declared

3. **PostgreSQL insertion errors**
   - Verify table schema exists
   - Check data type compatibility
   - Review constraint violations

### Debugging
```bash
# Check ATM terminals in DB2
SELECT * FROM AGENT_TERMINAL WHERE TERMINAL_TYPE IN ('ATM', 'CDM', 'MULTI');

# Check PostgreSQL table
SELECT COUNT(*) FROM "atmInformation";

# Monitor RabbitMQ queue
python check_queue_status.py
```

## Performance Considerations

### Optimization Tips
1. **Batch Size**: Adjust based on system resources (default: 1000)
2. **Poll Interval**: Balance between real-time and system load
3. **Connection Pooling**: Reuse database connections
4. **Index Usage**: Ensure proper indexing on timestamp columns

### Monitoring Metrics
- Records processed per batch
- Processing time per batch
- Queue depth in RabbitMQ
- Database connection usage
- Error rates and types

## Integration with Existing Pipelines

The ATM pipeline follows the same patterns as:
- **Cash Pipeline**: Similar financial data structure
- **Branch Pipeline**: Similar location and tracking logic
- **Agent Pipeline**: Similar terminal/location mapping

This ensures consistency across all BOT project pipelines and allows for shared monitoring and maintenance procedures.