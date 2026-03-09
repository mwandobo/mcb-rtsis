# Card Product Pipeline

This pipeline extracts card product (BIN) data from DB2 and streams it to PostgreSQL.

## Source
- **DB2 Table**: CMS_CARD, GENERIC_DETAIL
- **SQL File**: `card-product.sql`

## Target
- **PostgreSQL Table**: `cardProduct`

## Fields Extracted
| Field | Description |
|-------|-------------|
| reportingDate | Current timestamp in DDMMYYYYHHMM format |
| binNumber | First 6 digits of card number (BIN) |
| binNumberStartDate | Earliest card creation/production date |
| currency | TZS |
| cardType | Debit, Credit, or Prepaid |
| cardTypeSubCategory | CreditCard for credit cards |
| cardSchemeName | VISA |
| cardIssuerCategory | Domestic |
| cardIssuer | Mwalimu Commercial Bank Plc |

## Running the Pipeline

```bash
# Create the target table
python create_card_product_table.py

# Run the streaming pipeline
python run_card_product_pipeline.py
```

## Queue Configuration
- **RabbitMQ Queue**: `card_product_queue`
- **Dead Letter Queue**: `card_product_dead_letter