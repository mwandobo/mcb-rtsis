# Investment Debt Securities Pipeline

This folder contains the streaming pipeline for investment debt securities data processing.

## Files

- `investment_debt_securities_streaming_pipeline.py` - Main streaming pipeline with producer and consumer threads
- `create_investment_debt_securities_table.py` - PostgreSQL table creation script
- `run_investment_debt_securities_pipeline.py` - Pipeline runner script
- `clear_investment_debt_securities_queue.py` - Utility to clear RabbitMQ queue
- `test_investment_debt_securities_pipeline.py` - Comprehensive test suite
- `verify_data_quality.py` - Data quality verification tool

## Usage

### 1. Create the PostgreSQL table
```bash
python create_investment_debt_securities_table.py
```

### 2. Run the test suite
```bash
python test_investment_debt_securities_pipeline.py
```

### 3. Run the streaming pipeline
```bash
python run_investment_debt_securities_pipeline.py
```

### 4. Verify data quality
```bash
python verify_data_quality.py
```

### 5. Clear the queue (if needed)
```bash
python clear_investment_debt_securities_queue.py
```

## Pipeline Features

- **Streaming Architecture**: Producer and consumer run simultaneously for optimal performance
- **Batch Processing**: Configurable batch sizes for both DB2 queries and PostgreSQL inserts
- **Error Handling**: Retry logic with dead-letter queues for failed messages
- **Duplicate Prevention**: Unique index on securityNumber with ON CONFLICT handling
- **Progress Monitoring**: Real-time progress reports and statistics with accurate percentage calculation
- **Connection Management**: Persistent connections with automatic reconnection

## Configuration

The pipeline uses the same configuration as other pipelines in the system:
- DB2 connection settings
- PostgreSQL connection settings  
- RabbitMQ connection settings

## Data Source

The pipeline processes data from the `investment_debt_securities.sql` query, which extracts data from:
- `TRS_DEAL_RECORDING` (main treasury deal recording table)
- `TREASURY_MM_DEAL` (treasury money market deals)
- `TRS_DEAL_COLLATERAL` (collateral information)
- `CURRENCY` (currency information)
- `COLLABORATION_BANK` (bank information)
- `CUSTOMER` (customer/issuer information)
- `PRODUCT` (product information)
- `CUST_EXT_RATING` (external credit ratings)
- `COUNTRIES_LOOKUP` (country information)
- `TRS_MARKET_PRICE` (market pricing data)
- `FIXING_RATE` (exchange rates for currency conversion)

## Table Structure

The target PostgreSQL table `investmentDebtSecurities` contains 29 fields including:
- Security identification and classification
- Issuer information and ratings
- Multi-currency amount fields (cost, face, fair value)
- Trading intent and encumbrance status
- Dates (purchase, value, maturity)
- Risk classification and provisions
- Audit fields (created_at, updated_at)

## Key Features

### Security Classification
- **Security Types**: Treasury bonds, corporate bonds, government securities
- **Trading Intent**: Held to Maturity, Available for Sale, Trading Securities
- **Encumbrance Status**: Encumbered vs Unencumbered securities

### Multi-Currency Support
- **Cost Value**: Original purchase amounts
- **Face Value**: Nominal/par value amounts  
- **Fair Value**: Market-adjusted values using latest market prices
- **Currency Conversion**: Automatic USD and TZS conversion using fixing rates

### Risk Management
- **External Ratings**: AAA to Below B- rating categories
- **Internal Grades**: Grade A/B for unrated institutions
- **Asset Classification**: Current, Watch, Substandard based on maturity
- **Sector Classification**: SNA sector mapping for regulatory reporting

### Issuer Analysis
- **Issuer Information**: Name, country, sector classification
- **Rating Status**: Rated vs unrated issuers
- **Central Bank**: Special handling for central bank securities
- **Country Risk**: Resident vs non-resident issuer classification

## Data Quality Features

### Validation Rules
- Required fields: securityNumber, securityIssuerName, currency
- Unique constraint on securityNumber prevents duplicates
- Data type validation for numeric fields
- Date format validation

### Quality Metrics
- Completeness percentage for all fields
- Duplicate detection and reporting
- Currency and sector distribution analysis
- Interest rate and amount range validation
- Maturity date consistency checks

## Performance Considerations

- Uses streaming architecture for large datasets
- Implements batch processing for efficient database operations
- Includes connection pooling and retry mechanisms
- Provides real-time progress monitoring with accurate ETA calculations
- Handles complex JOINs and aggregations efficiently

## Regulatory Compliance

- Follows Bank of Tanzania (BOT) reporting requirements
- Maps to regulatory tables (D27, D67, D68) as specified
- Supports SNA sector classification for central bank reporting
- Includes all required fields for debt securities reporting
- Handles both rated and unrated securities appropriately