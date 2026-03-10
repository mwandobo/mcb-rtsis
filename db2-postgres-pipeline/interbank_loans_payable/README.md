# Interbank Loans Payable Pipeline

This folder contains the streaming pipeline for interbank loans payable data processing.

## Files

- `interbank_loans_payable_streaming_pipeline.py` - Main streaming pipeline with producer and consumer threads
- `create_interbank_loans_payable_table.py` - PostgreSQL table creation script
- `run_interbank_loans_payable_pipeline.py` - Pipeline runner script
- `clear_interbank_loans_payable_queue.py` - Utility to clear RabbitMQ queue

## Usage

### 1. Create the PostgreSQL table
```bash
python create_interbank_loans_payable_table.py
```

### 2. Run the streaming pipeline
```bash
python run_interbank_loans_payable_pipeline.py
```

### 3. Clear the queue (if needed)
```bash
python clear_interbank_loans_payable_queue.py
```

## Pipeline Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Batch Processing**: Configurable batch sizes for both DB2 queries and PostgreSQL inserts
- **Error Handling**: Retry logic with dead-letter queues for failed messages
- **Duplicate Prevention**: Unique index on accountNumber with ON CONFLICT handling
- **Progress Monitoring**: Real-time progress reports and statistics
- **Connection Management**: Persistent connections with automatic reconnection

## Configuration

The pipeline uses the same configuration as other pipelines in the system:
- DB2 connection settings
- PostgreSQL connection settings  
- RabbitMQ connection settings

## Data Source

The pipeline processes data from the `interbank_loans_payable.sql` query, which extracts data from:
- `TREASURY_MM_DEAL` (main treasury money market deals table)
- `COLLABORATION_BANK` (lender bank information)
- `CURRENCY` (currency information)
- `COUNTRIES_LOOKUP` (country name lookup)
- `FXFT_GLI_INTERFACE` (repayment transactions)
- `FIXING_RATE` (exchange rates for currency conversion)

## Table Structure

The target PostgreSQL table `interbankLoansPayable` contains 22 fields including:
- Reporting and transaction dates
- Lender information (name, country)
- Account/deal number
- Borrowing type and currency
- Amount information in original, USD, and TZS currencies
- Opening, repayment, and closing amounts
- Tenure and interest rate details
- Audit fields (created_at, updated_at)

## Key Features

### Amount Calculations
- **Opening Amounts**: Original loan disbursement amounts
- **Repayment Amounts**: Aggregated repayments from GL interface
- **Closing Amounts**: Opening minus repayments (outstanding balance)

### Currency Conversion
- Supports multi-currency loans with automatic USD and TZS conversion
- Uses latest fixing rates from the FIXING_RATE table
- Handles both foreign currency and local currency (TZS) loans

### Loan Details
- Tracks tenure in days
- Records annual interest rates
- Identifies fixed vs floating rate loans
- Links to collaboration banks for lender information

## Filters Applied

- Only borrowing transactions (`DEAL_OPERATION = 'B'`)
- Excludes cancelled deals (`STATUS != 'C'`)
- Focuses on interbank loans payable (money market borrowings)

## Performance Considerations

- Uses streaming architecture for large datasets
- Implements batch processing for efficient database operations
- Includes connection pooling and retry mechanisms
- Provides real-time progress monitoring and ETA calculations