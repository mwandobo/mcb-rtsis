# Income Statement Solution - BOT API Compliance

## Overview
This solution restructures the income statement data to meet BOT API requirements, which mandate both detailed item code breakdowns and total values for each category.

## Architecture

### 1. SQL Query (`sqls/income-statement.sql`)
The query returns data in a format optimized for transformation:

**Output Format:**
- `reportingDate`: DDMMYYYYHHMM format
- `interestIncome`: Comma-separated string "itemCode:value,itemCode:value,..."
- `interestIncomeValue`: Total sum (DECIMAL)
- `interestExpenses`: Comma-separated string
- `interestExpensesValue`: Total sum (DECIMAL)
- And so on for all categories...

**Example Output:**
```
reportingDate: "151220241430"
interestIncome: "1:1234567.89,2:987654.32,6:456789.01"
interestIncomeValue: 2679011.22
```

### 2. PostgreSQL Table Schema
```sql
CREATE TABLE "incomeStatement" (
    "id" SERIAL PRIMARY KEY,
    "reportingDate" VARCHAR(20) UNIQUE,
    "interestIncome" JSONB,              -- Array of {itemCode, value}
    "interestIncomeValue" DECIMAL(31,2),
    "interestExpenses" JSONB,
    "interestExpensesValue" DECIMAL(31,2),
    "badDebtsWrittenOffNotProvided" DECIMAL(31,2),
    "provisionBadDoubtfulDebts" DECIMAL(31,2),
    "impairmentsInvestments" DECIMAL(31,2),
    "incomeTaxProvision" DECIMAL(31,2),
    "extraordinaryCreditsCharge" DECIMAL(31,2),
    "nonCoreCreditsCharges" JSONB,
    "nonCoreCreditsChargesValue" DECIMAL(31,2),
    "nonInterestIncome" JSONB,
    "nonInterestIncomeValue" DECIMAL(31,2),
    "nonInterestExpenses" JSONB,
    "nonInterestExpensesValue" DECIMAL(31,2)
);
```

### 3. Processor Logic
The processor (`income_statement_processor.py`) converts comma-separated strings to JSONB arrays:

**Input:** `"1:1234567.89,2:987654.32,6:456789.01"`

**Output (JSONB):**
```json
[
  {"itemCode": 1, "value": 1234567.89},
  {"itemCode": 2, "value": 987654.32},
  {"itemCode": 6, "value": 456789.01}
]
```

## BOT API Mapping

### D46: Interest Income Item Codes
- **1**: Interest on loans, advances, and overdrafts
- **2**: Interest on interbank loans
- **3**: Interest on investment in govt debt securities
- **6**: Interest on balance with other banks
- **11**: Other interest income

### D47: Interest Expenses Item Codes
- **1**: Interest on public deposits
- **5**: Interest on govt deposits
- **11**: Other interest expenses

### D48: Non-Interest Income Item Codes
- **1**: Fees and commissions
- **4**: Foreign exchange gains
- **5**: Other non-interest income
- **20**: Miscellaneous income

### D49: Non-Interest Expenses Item Codes
- **1**: Staff costs
- **5**: Directors' emoluments
- **7**: Depreciation
- **8**: Amortization
- **9**: Audit fees
- **12**: Rent and rates
- **13**: Insurance
- **23**: IT expenses
- **24**: Marketing and advertising
- **25**: Other operating expenses
- **36**: Legal and professional fees
- **37**: Communication costs
- **39**: Training and development
- **40**: Other expenses

### D50: Non-Core Credits and Charges Item Codes
- **1**: Gain/loss on disposal of assets
- **3**: Other non-core items

## GL Account Mappings

### Interest Income (D46)
- **Item 1**: 400010001, 400010005, 400020001, 400020012, 400030001, 400030002, 400030003, 400030005, 400040001, 400040002, 400040003, 400040006, 400030007, 400030009
- **Item 2**: 408010001
- **Item 3**: 404020001
- **Item 6**: 400060001, 400060002, 400060005, 400060004, 400061001, 400070003
- **Item 11**: All accounts starting with 401, 402, 403

### Interest Expenses (D47)
- **Item 1**: 600220001-600220010
- **Item 5**: 602010001
- **Item 11**: 600230001-600230006, 600240001, 601060001

### Single Value Fields
- **badDebtsWrittenOffNotProvided**: GL 705190003
- **provisionBadDoubtfulDebts**: GL 705190002
- **impairmentsInvestments**: GL 705190001
- **incomeTaxProvision**: GL 704010001, 704020001-704020013, 704030004
- **extraordinaryCreditsCharge**: GL 505000000

## API Submission Format

When submitting to BOT API, the data structure will be:

```json
{
  "reportingDate": "151220241430",
  "interestIncome": [
    {"itemCode": 1, "value": 1234567.89},
    {"itemCode": 2, "value": 987654.32},
    {"itemCode": 6, "value": 456789.01}
  ],
  "interestIncomeValue": 2679011.22,
  "interestExpenses": [
    {"itemCode": 1, "value": 123456.78},
    {"itemCode": 5, "value": 98765.43}
  ],
  "interestExpensesValue": 222222.21,
  "badDebtsWrittenOffNotProvided": 50000.00,
  "provisionBadDoubtfulDebts": 100000.00,
  "impairmentsInvestments": 75000.00,
  "incomeTaxProvision": 250000.00,
  "extraordinaryCreditsCharge": 10000.00,
  "nonCoreCreditsCharges": [
    {"itemCode": 1, "value": 5000.00}
  ],
  "nonCoreCreditsChargesValue": 5000.00,
  "nonInterestIncome": [
    {"itemCode": 1, "value": 500000.00},
    {"itemCode": 4, "value": 250000.00}
  ],
  "nonInterestIncomeValue": 750000.00,
  "nonInterestExpenses": [
    {"itemCode": 1, "value": 800000.00},
    {"itemCode": 5, "value": 150000.00}
  ],
  "nonInterestExpensesValue": 950000.00
}
```

## Benefits

1. **API Compliance**: Matches BOT requirements exactly with both detailed breakdowns and totals
2. **Flexibility**: JSONB columns allow easy querying and transformation
3. **Performance**: GIN indexes on JSONB columns enable fast searches
4. **Maintainability**: Clear separation between data storage and API transformation
5. **Auditability**: Stores both granular and aggregated data for reconciliation

## Usage

### 1. Create Table
```bash
python create_income_statement_table.py
```

### 2. Run Pipeline
```bash
python income_statement_pipeline.py
```

### 3. Query Data
```sql
-- Get latest income statement
SELECT * FROM "incomeStatement" 
ORDER BY "reportingDate" DESC 
LIMIT 1;

-- Query specific item codes
SELECT 
    "reportingDate",
    jsonb_array_elements("interestIncome") as item
FROM "incomeStatement"
WHERE "reportingDate" = '151220241430';

-- Get item code 1 from interest income
SELECT 
    "reportingDate",
    item->>'itemCode' as code,
    item->>'value' as value
FROM "incomeStatement",
     jsonb_array_elements("interestIncome") as item
WHERE item->>'itemCode' = '1';
```

## Notes

- The query uses `RTRIM(..., ',')` to remove trailing commas from concatenated strings
- Empty categories (with 0 values) are excluded from the arrays to reduce data size
- All monetary values use DECIMAL(31,2) for precision
- The `reportingDate` field has a UNIQUE constraint to prevent duplicate submissions
- JSONB format allows direct JSON export for API submission without additional transformation
