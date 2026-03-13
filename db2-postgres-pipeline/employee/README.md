# Employee Streaming Pipeline

This pipeline extracts employee data from DB2 using the `employee-v1.sql` query and loads it into PostgreSQL using a streaming architecture with RabbitMQ for message queuing.

## Features

- **Streaming Architecture**: Producer and Consumer run simultaneously for real-time processing
- **Employee Data Processing**: Comprehensive employee information including salary calculations
- **Salary Generation**: Uses CTE with random salary generation between 400,000 and 3,000,000 TZS
- **Benefit Calculations**: Automatic NHIF (10%) and PSSSF (15%) calculations
- **Position Categorization**: Automatic categorization of senior vs non-senior management
- **Duplicate Prevention**: Uses ON CONFLICT to handle duplicate employee records
- **Error Handling**: Dead-letter queue for failed messages
- **Batch Processing**: Configurable batch sizes for optimal performance
- **Progress Monitoring**: Real-time progress reporting and ETA calculations

## Files

- `employee_streaming_pipeline.py` - Main pipeline implementation
- `run_employee_pipeline.py` - Pipeline runner script
- `create_employee_table.py` - PostgreSQL table creation script
- `clear_employee_queue.py` - Queue management utility
- `README.md` - This documentation

## Data Structure

The pipeline processes the following employee fields:

### Input (from employee-v1.sql)
- Employee personal information (name, gender, DOB)
- Identification details (type, number)
- Position and department information
- Branch assignment
- Salary calculations with benefits

### Output (PostgreSQL employeesInformation table)
- `reportingDate` - Current timestamp in DDMMYYYYHHMM format
- `branchCode` - Employee's branch code
- `empName` - Full employee name
- `gender` - Male/Female/Not Applicable
- `empDob` - Date of birth in DDMMYYYYHHMM format
- `empIdentificationType` - Always 'NationalIdentityCard'
- `empIdentificationNumber` - National ID number (unique)
- `empPosition` - Job position description
- `empPositionCategory` - Senior/Non-Senior management
- `empStatus` - Always 'Permanent and pensionable'
- `empDepartment` - Department name
- `appointmentDate` - Appointment date
- `empNationality` - Always 'TANZANIA, UNITED REPUBLIC OF'
- `lastPromotionDate` - Last promotion date
- `basicSalary` - Random salary between 400K-3M TZS
- `nhif` - NHIF contribution (10% of basic salary)
- `psssf` - PSSSF contribution (15% of basic salary)

## Usage

### 1. Create the PostgreSQL table
```bash
python create_employee_table.py
```

### 2. Run the pipeline
```bash
python run_employee_pipeline.py
```

### 3. Clear queue (if needed)
```bash
python clear_employee_queue.py
```

## Configuration

The pipeline uses the following default settings:
- **Batch Size**: 1000 records per DB2 fetch
- **Consumer Batch Size**: 100 records per PostgreSQL insert
- **Queue Name**: `employee_queue`
- **Dead Letter Queue**: `employee_dead_letter`

## SQL Query Details

The `employee-v1.sql` query includes:

1. **Salary CTE**: Generates random salaries for employees with STAFF_NO like 'EIC%'
2. **Employee Joins**: Links BANKEMPLOYEE with CUSTOMER for personal details
3. **Position Mapping**: Maps position codes to descriptions
4. **Department Mapping**: Maps department codes to descriptions
5. **Identification**: Extracts national ID numbers with 20-character validation
6. **Filtering**: Only active employees (EMPL_STATUS = 1) with EIC staff numbers

## Performance

- Processes employees in configurable batches
- Uses persistent PostgreSQL connections
- Implements connection retry logic
- Provides real-time progress monitoring
- Handles large datasets efficiently

## Error Handling

- Failed messages are sent to dead-letter queue
- PostgreSQL connection failures trigger reconnection
- RabbitMQ connection issues are automatically retried
- Comprehensive logging for troubleshooting

## Monitoring

The pipeline provides:
- Real-time progress updates
- Processing rate calculations
- ETA estimations
- Batch completion statistics
- Final summary report