# DB2 Compatibility Fixes for Underwriting Accounts Query

## Issue
DB2 execution error: `[-4476] [jcc][10103][10941][4.23.42] Method executeQuery cannot be used for update`

## Root Cause Analysis
This error typically occurs when:
1. DB2 interprets the query as potentially modifying data
2. Complex UNION ALL statements cause parsing issues
3. Certain function combinations trigger update detection
4. Date arithmetic or COALESCE functions in specific contexts

## Solutions Implemented

### 1. Split UNION ALL Query
**Problem**: Large UNION ALL queries can trigger DB2 update detection
**Solution**: Split into separate queries:
- `underwritingAccounts_DB2_COMPATIBLE.sql` - HIST_SO_COMMITMENT only
- `underwritingAccounts_TP_COMMITMENT.sql` - TP_SO_COMMITMENT only

### 2. Replace COALESCE with CASE Statements
**Problem**: COALESCE can trigger update detection in some DB2 versions
**Solution**: 
```sql
-- Before (problematic)
COALESCE(h.ACTIVATION_DATE, h.EXECUTION_DATE)

-- After (DB2 compatible)
CASE 
    WHEN h.ACTIVATION_DATE IS NOT NULL THEN h.ACTIVATION_DATE
    ELSE h.EXECUTION_DATE
END
```

### 3. Simplify Date Arithmetic
**Problem**: Complex date calculations can cause issues
**Solution**:
```sql
-- Before (problematic)
h.EXECUTION_DATE < CURRENT_DATE - 180 DAYS

-- After (DB2 compatible)
h.EXECUTION_DATE < (CURRENT_DATE - 180 DAYS)
```

### 4. Use Explicit Date Functions
**Problem**: Implicit date conversions
**Solution**:
```sql
-- Before
h.ACTIVATION_DATE >= '2023-01-01'

-- After
h.ACTIVATION_DATE >= DATE('2023-01-01')
```

### 5. Replace TRIM with RTRIM
**Problem**: TRIM function variations
**Solution**:
```sql
-- Before
TRIM(c.SURNAME)

-- After
RTRIM(c.SURNAME)
```

### 6. Simplify ROUND to INTEGER
**Problem**: ROUND function complexity
**Solution**:
```sql
-- Before
ROUND(h.PAYMENT_AMOUNT / 1000, 0)

-- After
INTEGER(h.PAYMENT_AMOUNT / 1000)
```

## Testing Strategy

### Step 1: Minimal Test
Run `underwritingAccounts_MINIMAL_TEST.sql` first to verify basic connectivity.

### Step 2: Single Table Queries
Test each table separately:
1. `underwritingAccounts_DB2_COMPATIBLE.sql` (HIST_SO_COMMITMENT)
2. `underwritingAccounts_TP_COMMITMENT.sql` (TP_SO_COMMITMENT)

### Step 3: Combine Results
If both work individually, combine results in application layer or create a view.

## Expected Results
- **HIST_SO_COMMITMENT**: ~10,666 records, 10.57B TZS (Service Product 9300: 1,526 records)
- **TP_SO_COMMITMENT**: ~1,045 records, 2.05B TZS (Service Product 9102: 1,027 records)
- **Total Combined**: ~11,711 records, 12.62B TZS

## Fallback Options

If issues persist:

### Option 1: Create Temporary View
```sql
CREATE VIEW UNDERWRITING_COMBINED AS
SELECT * FROM (
    -- HIST_SO_COMMITMENT query here
) 
UNION ALL
SELECT * FROM (
    -- TP_SO_COMMITMENT query here
);
```

### Option 2: Use WITH Clause
```sql
WITH hist_data AS (
    -- HIST_SO_COMMITMENT query
),
tp_data AS (
    -- TP_SO_COMMITMENT query
)
SELECT * FROM hist_data
UNION ALL
SELECT * FROM tp_data;
```

### Option 3: Application-Level Combination
Execute both queries separately and combine results in the application.

## BOT Compliance
All queries maintain full BOT compliance with:
- All 11 mandatory fields present
- Correct D88, D42, D32 lookup values (text descriptions, not codes)
- Proper date filtering (2023+)
- High-value transaction focus
- Asset classification logic