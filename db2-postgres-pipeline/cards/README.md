# Cards Streaming Pipeline

This pipeline extracts card information from DB2 (CMS_CARD table) and loads it into PostgreSQL using a streaming architecture with RabbitMQ as the message queue.

## Data Source

- **SQL Query**: `sqls/card_information.sql`
- **Source Table**: CMS_CARD
- **Target Table**: cardInformation (PostgreSQL)

## Architecture

The pipeline uses a streaming architecture with:
- **Producer**: Reads from DB2 and publishes to RabbitMQ queue
- **Consumer**: Reads from RabbitMQ and batch inserts to PostgreSQL
- **Message Queue**: RabbitMQ with dead-letter queue for failed messages

Both producer and consumer run simultaneously for optimal performance.

## Data Fields (16 fields)

1. reportingDate - Current timestamp
2. bankCode - Bank code (MWCOTZTZ)
3. cardNumber - Full card number
4. binNumber - Last 10 digits of card number
5. customerIdentificationNumber - Customer ID
6. cardType - Card type (Debit)
7. cardTypeSubCategory - Card type subcategory
8. cardIssueDate - Card issue date
9. cardIssuer - Card issuer name
10. cardIssuerCategory - Issuer category (Domestic)
11. cardIssuerCountry - Issuer country
12. cardHolderName - Card holder name
13. cardStatus - Card status (Active/Inactive)
14. cardScheme - Card scheme (VISA)
15. acquiringPartner - Acquiring partner name
16. cardExpireDate - Card expiry date

## Files

- `cards_streaming_pipeline.py` - Main streaming pipeline (producer + consumer)
- `create_cards_table.py` - Creates PostgreSQL table with indexes
- `run_cards_pipeline.py` - Runner script (creates table + runs pipeline)
- `clear_cards_queue.py` - Clears RabbitMQ queue
- `README.md` - This file

## Usage

### Quick Start (Recommended)

Run the complete pipeline with one command:

```bash
python run_cards_pipeline.py
```

This will:
1. Create the cardInformation table in PostgreSQL
2. Run the streaming pipeline (producer + consumer simultaneously)

### Individual Steps

If you need more control:

```bash
# 1. Create table
python create_cards_table.py

# 2. Run streaming pipeline
python cards_streaming_pipeline.py
```

### Clear Queue

If you need to restart or clear stuck messages:

```bash
python clear_cards_queue.py
```

## Configuration

The pipeline uses configuration from the parent `config.py`:
- DB2 connection settings
- PostgreSQL connection settings
- RabbitMQ connection settings

## Performance

- **Batch Size**: 1000 records per DB2 fetch
- **Consumer Batch Size**: 100 records per PostgreSQL insert
- **Duplicate Prevention**: Unique index on cardNumber
- **Error Handling**: Dead-letter queue for failed messages
- **Retry Logic**: 3 retries with 5-second delay

## Monitoring

The pipeline provides:
- Real-time progress updates
- Records processed count
- Processing rate (records/second)
- Success rate percentage
- ETA for completion
- Detailed progress reports every 5 minutes

## Error Handling

- Failed messages are sent to dead-letter queue
- PostgreSQL connection auto-reconnect
- RabbitMQ connection retry logic
- Batch insert rollback on errors

## Table Structure

```sql
CREATE TABLE "cardInformation" (
    "id" SERIAL PRIMARY KEY,
    "reportingDate" VARCHAR(50),
    "bankCode" VARCHAR(50),
    "cardNumber" VARCHAR(100) NOT NULL,
    "binNumber" VARCHAR(50),
    "customerIdentificationNumber" VARCHAR(100),
    "cardType" VARCHAR(50),
    "cardTypeSubCategory" VARCHAR(100),
    "cardIssueDate" VARCHAR(50),
    "cardIssuer" VARCHAR(200),
    "cardIssuerCategory" VARCHAR(100),
    "cardIssuerCountry" VARCHAR(100),
    "cardHolderName" VARCHAR(200),
    "cardStatus" VARCHAR(50),
    "cardScheme" VARCHAR(100),
    "acquiringPartner" VARCHAR(200),
    "cardExpireDate" VARCHAR(50),
    "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

## Indexes

- Unique index on `cardNumber` (for duplicate prevention)
- Index on `customerIdentificationNumber` (for customer queries)
- Index on `cardStatus` (for status filtering)
- Index on `cardType` (for type filtering)
