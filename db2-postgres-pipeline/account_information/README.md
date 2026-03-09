# Account Information Pipeline

Streaming pipeline that extracts account information from DB2 PROFITS database and loads it into PostgreSQL.

## Data Source

- **SQL Query**: `sqls/account-information.sql`
- **Main Tables**: 
  - PROFITS_ACCOUNT (main account data)
  - W_DIM_CUSTOMER (customer information)
  - CURRENCY (currency lookup)
  - STAT_ACCOUNT_BAL (deposit balances)
  - LOAN_NRM_VAL_BAL (loan balances)
  - fixing_rate (exchange rates for currency conversion)

## Fields Extracted

1. **reportingDate** - Date of pushing the report (DDMMYYYYHHMM format)
2. **customerIdentificationNumber** - Customer unique ID
3. **accountNumber** - Bank account number or MSISDN
4. **accountProductCode** - Internal product code with currency
5. **accountOperationStatus** - Account status (active, closed, dormant, inactive, abandoned, non contributing)
6. **customerType** - Nature/legal form of customer (Individual, Private company)
7. **smrCode** - SMR computation identifier (Not Applicable, Non-Government, Others)
8. **status** - Account withheld due to KYC issues (Y/N)
9. **orgAccountBalance** - Original balance in account currency
10. **usdAccountBalance** - Balance converted to USD
11. **tzsAccountBalance** - Balance converted to TZS

## Setup

1. Create the PostgreSQL table:
```bash
python create_account_information_table.py
```

2. Run the streaming pipeline:
```bash
python run_account_information_pipeline.py
```

## Pipeline Architecture

- **Mode**: Streaming (Producer + Consumer run simultaneously)
- **Producer**: Fetches records from DB2 in batches and publishes to RabbitMQ
- **Consumer**: Consumes messages from RabbitMQ and batch inserts to PostgreSQL
- **Queue**: `account_information_queue`
- **Dead Letter Queue**: `account_information_dead_letter`

## Configuration

- **Batch Size**: 1000 records per DB2 fetch
- **Consumer Batch Size**: 100 records per PostgreSQL insert
- **Duplicate Handling**: ON CONFLICT UPDATE on accountNumber (updates existing records)

## Features

- Streaming architecture for memory efficiency
- Automatic retry logic for RabbitMQ and PostgreSQL connections
- Dead-letter queue for failed messages
- Progress tracking with ETA calculation
- Currency conversion (TZS, USD, and other currencies)
- Handles both deposit and loan account balances
- Filters out dummy accounts and specific product types

## Monitoring

The pipeline logs:
- Connection status for DB2, PostgreSQL, and RabbitMQ
- Batch processing progress with percentage complete
- Records per second processing rate
- ETA for completion
- Error messages with context

## Notes

- The pipeline uses ON CONFLICT UPDATE to handle duplicate accountNumber entries
- Exchange rates are fetched from the fixing_rate table for currency conversions
- Latest customer records are used (ROW_NUMBER partitioning)
- Latest loan balances are used (ROW_NUMBER partitioning)
