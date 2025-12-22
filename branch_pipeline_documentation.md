# Branch Pipeline Documentation

## Overview
Successfully created a complete data pipeline for branch information from DB2 to PostgreSQL, including processor, configuration, and testing.

## Components Created

### 1. Branch Processor (`db2-postgres-pipeline/processors/branch_processor.py`)
- **Purpose**: Processes branch data from DB2 and transforms it for PostgreSQL
- **Features**:
  - Converts DB2 DDMMYYYYHHMM format to PostgreSQL timestamp/date formats
  - Validates branch records before insertion
  - Handles nullable fields properly
  - Transforms and normalizes data

### 2. Configuration (`db2-postgres-pipeline/config.py`)
- **Added**: Branch table configuration to the pipeline
- **Settings**:
  - Batch Size: 500 records
  - Poll Interval: 30 seconds
  - Queue Name: branch_queue
  - Processor Class: BranchProcessor

### 3. Test Suite (`db2-postgres-pipeline/test_branch_pipeline.py`)
- **Purpose**: Comprehensive testing of branch pipeline
- **Tests**:
  - Configuration validation
  - Record processing
  - Data transformation
  - PostgreSQL insertion
  - SQL query validation

## Data Flow

```
DB2 (UNIT table)
    ↓
SQL Query (branch1.sql)
    ↓
BranchProcessor
    ↓
Data Transformation
    ↓
PostgreSQL (branch table)
```

## Field Mapping

### Source Query Fields (from branch1.sql):
1. **reportingDate** - Current timestamp (DDMMYYYYHHMM)
2. **branchName** - From UNIT.UNIT_NAME
3. **taxIdentificationNumber** - From BANK_PARAMETERS
4. **businessLicense** - From GENERIC_DETAIL or generated
5. **branchCode** - From UNIT.CODE
6. **qrFsrCode** - From GENERIC_DETAIL or generated
7. **region** - From GENERIC_DETAIL (region lookup)
8. **district** - From BDG_DISTRICT or GENERIC_DETAIL
9. **ward** - From GENERIC_DETAIL (ward lookup)
10. **street** - From UNIT address fields
11. **houseNumber** - From UNIT.PLOT_STREET or BUILDING_UNIT
12. **postalCode** - From UNIT.ZIP_CODE or PO_BOX
13. **gpsCoordinates** - From UNIT.LATITUDE_LOCATION and LONGITUDE_LOCATION
14. **bankingServices** - From GENERIC_DETAIL (D104) or based on unit type
15. **mobileMoneyServices** - From GENERIC_DETAIL (D70)
16. **registrationDate** - From UNIT.OPEN_DATE (DDMMYYYYHHMM)
17. **branchStatus** - Based on UNIT.ENTRY_STATUS and INACTIVE_UNIT
18. **closureDate** - Conditional based on status (DDMMYYYYHHMM)
19. **contactPerson** - From BANKEMPLOYEE
20. **telephoneNumber** - From UNIT.TELEPHONE_1 or TELEPHONE_2
21. **altTelephoneNumber** - From UNIT.TELEPHONE_2 or FAX
22. **branchCategory** - From GENERIC_DETAIL or based on unit type

### Target PostgreSQL Fields:
All fields mapped 1:1 with proper data type conversions:
- **TIMESTAMP** fields: reportingDate
- **DATE** fields: registrationDate, closureDate
- **VARCHAR** fields: All text fields
- **TEXT** fields: bankingServices, mobileMoneyServices

## Date/Time Conversion

### DB2 Format → PostgreSQL Format:
- **DDMMYYYYHHMM** → **YYYY-MM-DD HH:MM:SS** (for TIMESTAMP)
- **DDMMYYYYHHMM** → **YYYY-MM-DD** (for DATE)

### Example:
- DB2: `191220251430` → PostgreSQL: `2025-12-19 14:30:00`
- DB2: `150120201200` → PostgreSQL Date: `2020-01-15`

## Data Validation

### Required Fields:
- branchCode (must not be empty)
- branchName (must not be empty)
- branchStatus (must not be empty)
- region (must not be empty)

### Optional Fields:
- street
- houseNumber
- postalCode
- closureDate (only for closed branches)
- altTelephoneNumber

## Source Query Details

### Main Table:
- **UNIT** - Primary source for branch information

### Joined Tables:
- **BANK_PARAMETERS** - Bank-level information (TIN)
- **BDG_DISTRICT** - District information
- **BANKEMPLOYEE** - Branch manager/contact person
- **GENERIC_DETAIL** - Multiple lookups:
  - Region (FKGH_RESIDES_IN_RE)
  - District (FKGH_RESIDES_IN_R1)
  - Ward (FKGH_RESID_REGION3)
  - Category (FKGH_HAS_UNIT_CATE)
  - Services (D104)
  - Mobile Money (D70)
  - License (LICNS)
  - FSR Code (FSRCD)

### Filters:
- `ENTRY_STATUS = '1'` (Active entries)
- `CODE IS NOT NULL` (Valid branch codes)
- `INACTIVE_UNIT = '0'` (Operational branches)
- `FETCH FIRST 1000 ROWS ONLY` (Batch limit)

## Testing Results

### ✅ All Tests Passed:
1. **Configuration Test**: Branch configuration found and validated
2. **Processor Test**: Record processing successful
3. **Validation Test**: Record validation passed
4. **Transformation Test**: Data transformation working
5. **PostgreSQL Test**: Insertion successful
6. **Query Test**: SQL query validated

### Sample Test Output:
```
Branch Code: 001
Branch Name: Head Office
Region: Dar es Salaam
District: Ilala
Status: Active
Category: Head Office
Contact: John Mwalimu
```

## Usage

### Running the Pipeline:
```bash
# Test the pipeline
python test_branch_pipeline.py

# Run the full pipeline (when integrated)
python production_pipeline.py --table branch
```

### Configuration:
The branch pipeline is configured in `config.py`:
```python
'branch': TableConfig(
    name='branch',
    query="""...""",  # Full SQL query
    timestamp_column='reportingDate',
    target_table='branch',
    queue_name='branch_queue',
    processor_class='BranchProcessor',
    batch_size=500,
    poll_interval=30
)
```

## Performance Considerations

### Batch Processing:
- **Batch Size**: 500 records per batch
- **Poll Interval**: 30 seconds between polls
- **Query Limit**: 1000 rows per query execution

### Indexing:
PostgreSQL indexes created for optimal query performance:
- idx_branch_code (branchCode)
- idx_branch_name (branchName)
- idx_branch_status (branchStatus)
- idx_branch_region (region)
- idx_branch_district (district)

## Error Handling

### Date Conversion Errors:
- Falls back to current timestamp/date if conversion fails
- Handles None/NULL values gracefully

### Validation Errors:
- Records failing validation are logged and skipped
- Retry mechanism available through queue system

### Database Errors:
- Transaction rollback on failure
- Error logging for debugging
- Dead letter queue for failed records

## Integration with Existing Pipeline

### Processor Registration:
Added to `processors/__init__.py`:
```python
from .branch_processor import BranchProcessor, BranchRecord
```

### Queue System:
- **Main Queue**: branch_queue
- **Retry Queue**: branch_queue_retry
- **Dead Letter Queue**: branch_queue_dlq

## Monitoring and Maintenance

### Key Metrics to Monitor:
- Records processed per batch
- Processing time per record
- Validation failure rate
- Database insertion success rate
- Queue depth

### Maintenance Tasks:
- Regular validation of source data quality
- Monitoring of date conversion accuracy
- Review of failed records in DLQ
- Performance optimization based on metrics

## Future Enhancements

### Potential Improvements:
1. Add incremental loading based on last update timestamp
2. Implement change data capture (CDC) for real-time updates
3. Add data quality checks and alerts
4. Implement branch hierarchy tracking
5. Add geospatial queries using GPS coordinates
6. Create materialized views for reporting

## Troubleshooting

### Common Issues:

**Issue**: Date conversion errors
- **Solution**: Check DB2 date format matches DDMMYYYYHHMM

**Issue**: Missing branch data
- **Solution**: Verify UNIT table filters and joins

**Issue**: Validation failures
- **Solution**: Check required fields are populated in source

**Issue**: PostgreSQL connection errors
- **Solution**: Verify connection parameters in .env file

## Contact and Support

For issues or questions about the branch pipeline:
1. Check logs in `logs/pipeline.log`
2. Review test output from `test_branch_pipeline.py`
3. Verify configuration in `config.py`
4. Check PostgreSQL table structure matches schema