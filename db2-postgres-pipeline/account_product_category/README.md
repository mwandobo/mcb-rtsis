# Account Product Category Pipeline

This pipeline extracts account product category data from DB2 and streams it to PostgreSQL.

## Source
- **DB2 Tables**: W_DIM_PRODUCT, PRODUCT, PROFITS_ACCOUNT, CURRENCY
- **SQL File**: `account-product-category.sql`

## Target
- **PostgreSQL Table**: `accountProductCategory`

## Fields Extracted
| Field | Description |
|-------|-------------|
| reportingDate | Current timestamp in DDMMYYYYHHMM format |
| accountProductCode | Product code with currency suffix |
| accountProductName | Product description |
| accountProductDescription | Product description |
| accountProductType | Saving, Current, Fixed deposits, Call deposits, Others |
| accountProductSubType | Mobile money Trust accounts, Mobile money interest account, Normal |
| currency | USD, EUR, GBP, TZS |
| accountProductCreationDate | Product validity date |
| accountProductClosureDate | Product expiration date (when closed) |
| accountProductStatus | Active or Closed |

## Running the Pipeline

```bash
# Create the target table
python create_account_product_category_table.py

# Run the streaming pipeline
python run_account_product_category_pipeline.py
```

## Queue Configuration
- **RabbitMQ Queue**: `account_product_category_queue`
- **Dead Letter Queue**: `account_product_category_dead_letter`