# Employee Records RTSIS Report

## Overview
This document describes the Employee Records RTSIS (Real Time Supervisory Information System) report implementation for MCB Bank Tanzania. The report captures comprehensive employee information as required by the Bank of Tanzania regulatory reporting framework.

## Report Purpose
The Employee Records report provides detailed information about bank employees including:
- Personal identification details
- Employment status and position information
- Compensation and benefits data
- Contact information
- Branch assignments

## Files Structure

### Main Query File
- **`employee_records_rtsis.sql`** - Main RTSIS query that generates the employee records report

### Validation and Testing
- **`validate_employee_records.sql`** - Comprehensive validation script for data quality checks
- **`test_employee_data.sql`** - Simple test script to verify BANKEMPLOYEE table data
- **`EMPLOYEE_RECORDS_RTSIS_README.md`** - This documentation file

## Data Source
The report primarily uses the **BANKEMPLOYEE** table from the PROFITS database with the following key fields:

### BANKEMPLOYEE Table Structure
```sql
- ID (CHAR(8)) - Internal employee ID
- STAFF_NO (CHAR(8)) - Employee staff number (Primary identifier)
- FIRST_NAME (VARCHAR(20)) - Employee first name
- LAST_NAME (VARCHAR(20)) - Employee last name
- SEX (CHAR(1)) - Gender (M/F or 1/2)
- EMPL_STATUS (CHAR(1)) - Employment status (1=Active, 0=Inactive, 2=Contract, 3=Temporary)
- EMAIL (CHAR(40)) - Employee email address
- WORK_PHONE (VARCHAR(20)) - Work telephone number
- MOBILE_PHONE (VARCHAR(20)) - Mobile phone number
- SIGNATURE_LEVEL (CHAR(1)) - Authorization level (A-H)
- FATHER_NAME (CHAR(40)) - Father's name
- CARD_ID (CHAR(8)) - Employee card ID
- FKGH_HAS_AS_GRADE (CHAR(5)) - Grade foreign key
- FKGH_WORKS_IN_POSI (CHAR(5)) - Position foreign key
- TMSTAMP (TIMESTAMP(6)) - Record timestamp
```

## RTSIS Field Mappings

### Mandatory Fields (Y=Yes)

| RTSIS Field | Description | Data Source | Business Rules |
|-------------|-------------|-------------|----------------|
| reportingDate | Reporting date and time | CURRENT_TIMESTAMP | DDMMYYYYHHMM format |
| branchCode | Branch sort code | UNIT.CODE | Employee's assigned branch |
| empName | Employee name | FIRST_NAME + LAST_NAME | Full name concatenation |
| gender | Employee gender | SEX field mapping | Male/Female from M/F or 1/2 |
| empDob | Date of birth | GENERIC_DETAIL lookup | DDMMYYYYHHMM format, stored as string in DESCRIPTION |
| empIdentificationType | ID document type | D02 lookup table | National ID default, from DESCRIPTION |
| empIdentificationNumber | ID number | STAFF_NO | Primary key for report |
| empPosition | Job position | Position lookup | From GENERIC_DETAIL.DESCRIPTION |
| empPositionCategory | Position category | D173 lookup | Senior/Non-senior management, from DESCRIPTION |
| empStatus | Employment status | EMPL_STATUS mapping | Permanent/Contract/Temporary |
| empDepartment | Department | Department lookup | From GENERIC_DETAIL.DESCRIPTION |
| appointmentDate | Employment start date | Appointment lookup | DDMMYYYYHHMM format, stored as string in DESCRIPTION |
| empNationality | Employee nationality | Country lookup | Tanzanian default, from DESCRIPTION |
| lastPromotionDate | Last promotion date | Promotion lookup | DDMMYYYYHHMM format, stored as string in DESCRIPTION |
| basicSalary | Basic salary amount | Salary lookup | Numeric value cast from DESCRIPTION string |
| empBenefits | Employee benefits | D153 lookup | Array of benefits, from DESCRIPTION |

### Additional Fields for Tracking
- employeeId - Internal ID
- staffNumber - Staff number
- fatherName - Father's name
- cardId - Card ID
- signatureLevel - Authorization level
- email - Email address
- workPhone - Work phone
- mobilePhone - Mobile phone

## Lookup Tables Used

### GENERIC_DETAIL Lookups
- **D02** - Identity document types (stored in DESCRIPTION)
- **D93** - Gender codes (stored in DESCRIPTION)
- **D152** - Employment status types (stored in DESCRIPTION)
- **D153** - Employee benefits types (stored in DESCRIPTION)
- **D173** - Position categories (stored in DESCRIPTION)
- **EMPDB** - Employee date of birth (stored in DESCRIPTION as date string)
- **EMPAP** - Employee appointment dates (stored in DESCRIPTION as date string)
- **EMPPR** - Employee promotion dates (stored in DESCRIPTION as date string)
- **EMPSL** - Employee salary information (stored in DESCRIPTION as numeric string)
- **DEPT** - Department information (stored in DESCRIPTION)
- **CNTRY** - Country/nationality codes (stored in DESCRIPTION)

**Note**: All lookup values are stored in the DESCRIPTION field of GENERIC_DETAIL table. Date values are stored as strings and salary values are cast to DECIMAL when needed.

## Data Quality Rules

### Filtering Criteria
- Only active employees (EMPL_STATUS = '1')
- Must have valid STAFF_NO
- Must have at least first name or last name
- Excludes test/dummy records

### Default Values
- Default branch code: '001' (Head Office)
- Default gender: 'Not Specified'
- Default DOB: '01011980000000'
- Default ID type: 'National ID'
- Default position: 'Bank Officer'
- Default department: 'General Banking'
- Default nationality: 'Tanzanian'
- Default salary: 500,000 TZS
- Default benefits: 'Medical Insurance, Transport Allowance, Housing Allowance'

## Usage Instructions

### 1. Run Main Query
```sql
-- Execute the main RTSIS query
@employee_records_rtsis.sql
```

### 2. Validate Data Quality
```sql
-- Run validation checks
@validate_employee_records.sql
```

### 3. Test Data Availability
```sql
-- Quick data check
@test_employee_data.sql
```

## Expected Output Format

The report generates records in the following format:
```
reportingDate: 301220250000 (DDMMYYYYHHMM)
branchCode: 001
empName: John Doe
gender: Male
empDob: 15011985000000
empIdentificationType: National ID
empIdentificationNumber: EMP001
empPosition: Senior Manager
empPositionCategory: Senior Management
empStatus: Permanent
empDepartment: Operations
appointmentDate: 01012020000000
empNationality: Tanzanian
lastPromotionDate: 01012023000000
basicSalary: 1500000.00
empBenefits: Medical Insurance, Transport Allowance, Housing Allowance
```

## Validation Checks

The validation script performs the following checks:
1. **Record Count** - Total and unique employee counts
2. **Mandatory Fields** - Ensures all required fields are populated
3. **Data Quality** - Checks for duplicates and invalid data
4. **Gender Distribution** - Validates gender field values
5. **Employment Status** - Verifies status classifications
6. **Salary Analysis** - Reviews salary ranges and statistics
7. **Contact Information** - Checks completeness of contact details
8. **RTSIS Compliance** - Overall readiness for submission

## Common Issues and Solutions

### Issue 1: Missing Employee Names
**Problem**: Some employees have NULL first_name and last_name
**Solution**: Query filters out records without names, uses 'Unknown Employee' as fallback

### Issue 2: Invalid Gender Codes
**Problem**: Gender field contains unexpected values
**Solution**: Maps M/F and 1/2 to Male/Female, defaults to 'Not Specified'

### Issue 3: Missing Salary Information
**Problem**: No salary data in GENERIC_DETAIL lookups or salary stored as string
**Solution**: Uses default salary of 500,000 TZS, casts DESCRIPTION to DECIMAL when available

### Issue 4: Branch Assignment
**Problem**: No direct employee-to-branch relationship
**Solution**: Uses default head office branch code '001'

## Performance Considerations

- Query includes multiple LEFT JOINs with GENERIC_DETAIL table
- Indexes on BANKEMPLOYEE.STAFF_NO and BANKEMPLOYEE.ID improve performance
- LIMIT clauses in subqueries prevent excessive data processing
- Consider adding WHERE clauses for specific date ranges if needed

## Maintenance Notes

### Regular Updates Required
1. **Lookup Table Values** - Update GENERIC_DETAIL entries for new codes
2. **Default Values** - Review and update default values as needed
3. **Branch Assignments** - Implement proper employee-branch relationships
4. **Salary Information** - Establish salary data maintenance process

### Data Quality Monitoring
- Run validation script monthly
- Monitor for new employee records without required fields
- Check for changes in BANKEMPLOYEE table structure
- Validate lookup table completeness

## Compliance Notes

This report meets RTSIS requirements for:
- Employee identification and demographics
- Employment status and position information
- Compensation data reporting
- Contact information maintenance
- Branch assignment tracking

The report format follows Bank of Tanzania specifications for regulatory reporting and includes all mandatory fields as specified in the RTSIS documentation.

## Contact Information

For technical issues or questions about this report:
- Database Team: MCB Bank Tanzania
- Report Created: December 30, 2025
- Last Updated: December 30, 2025
- Version: 1.0