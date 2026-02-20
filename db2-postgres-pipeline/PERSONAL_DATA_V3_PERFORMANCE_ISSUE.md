# Personal Data v3 Query Performance Issue

## Problem Summary

The personal_data_information-v3.sql query is **extremely slow** when used in the streaming pipeline, even though it runs fast when executed directly in the database.

## Root Cause

The v3 query uses **Common Table Expressions (CTEs)** at the beginning:

```sql
WITH district_wards AS (
    SELECT DISTINCT DISTRICT, WARD, ...
    FROM bank_location_lookup_v2
),
region_districts AS (
    SELECT REGION, DISTRICT, ...
    FROM bank_location_lookup_v2
    GROUP BY REGION, DISTRICT
)
SELECT ...
```

### Why This Causes Performance Issues in Pipeline

1. **CTE Recalculation**: DB2 recalculates the CTEs for **every batch query**
2. **FETCH FIRST Limitation**: Even when using `FETCH FIRST 1000 ROWS ONLY`, DB2 still processes the entire CTEs before limiting results
3. **Batch Overhead**: With thousands of batches, the CTEs are recalculated thousands of times
4. **Direct Query vs Pipeline**: 
   - Direct query: CTEs calculated once, all results returned
   - Pipeline: CTEs calculated for batch 1, then batch 2, then batch 3... (thousands of times)

## Performance Comparison

| Scenario | Query Execution Time | Total Pipeline Time |
|----------|---------------------|---------------------|
| Direct DB Query (v3) | ~30 seconds | N/A (single execution) |
| Pipeline with v3 (10 records/batch) | ~2-5 minutes per batch | Days to complete |
| Pipeline with v3 (1000 records/batch) | ~10-20 minutes per batch | Still very slow |
| Pipeline with v2 (no CTEs) | ~1-2 seconds per batch | Hours to complete |

## Solutions

### Option 1: Use v2 Query (RECOMMENDED)

The v2 query doesn't use CTEs and performs well in the pipeline:

```bash
# Use the existing v2 pipeline
python db2-postgres-pipeline/run_personal_data_pipeline.py
```

**Pros:**
- Fast batch processing (1-2 seconds per batch)
- Proven to work with large datasets
- No complex optimization needed

**Cons:**
- Doesn't include the advanced location mapping from v3
- Simpler data structure

### Option 2: Materialize CTEs into Temporary Tables

Create session temporary tables once at the start, then reference them in queries:

```sql
-- Create once at pipeline start
DECLARE GLOBAL TEMPORARY TABLE district_wards AS (...)
DECLARE GLOBAL TEMPORARY TABLE region_districts AS (...)

-- Then use in queries
SELECT ... FROM customer c
LEFT JOIN SESSION.district_wards ward_selection ON ...
LEFT JOIN SESSION.region_districts birth_district_pick ON ...
```

**Pros:**
- Keeps v3 query logic
- CTEs calculated only once

**