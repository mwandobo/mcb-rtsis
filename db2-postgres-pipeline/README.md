# Multi-table DB2 to PostgreSQL Pipeline

## Overview
Real-time data pipeline that syncs multiple data types from DB2 to PostgreSQL using RabbitMQ queues:
- Cash Information
- Asset Owned  
- BOT Balances
- MNOs Balances
- Other Banks Balances
- Other Assets

## Core Files

### Main Pipeline
- `simple_multi_pipeline.py` - **Main multi-table pipeline** (run this)

### Configuration
- `config.py` - Pipeline configuration (tables, processors, connections)
- `.env` - Environment variables (DB2, PostgreSQL, RabbitMQ, Redis)
- `requirements.txt` - Python dependencies

### Database
- `db2_connection.py` - DB2 connection handler using pyodbc
- `sql/postgres-schema.sql` - PostgreSQL schema with camelCase columns

### Processors
- `processors/base.py` - Base processor class
- `processors/cash_processor.py` - Cash data processor
- `processors/assets_processor.py` - Assets data processor
- `processors/bot_balances_processor.py` - BOT balances processor
- `processors/mnos_processor.py` - MNOs balances processor
- `processors/other_banks_processor.py` - Other banks balances processor
- `processors/other_assets_processor.py` - Other assets processor

### Utilities
- `check_data.py` - Verify data in PostgreSQL tables

### Test Scripts
- `test_all_pipelines.py` - Test all processors
- `test_assets_pipeline.py` - Test assets processor
- `test_bot_pipeline.py` - Test BOT balances processor
- `test_other_assets_pipeline.py` - Test other assets processor

## Usage

### Run Multi-table Pipeline
```bash
python simple_multi_pipeline.py
```

### Check Data
```bash
python check_data.py
```

## Architecture
```
DB2 (GLI_TRX_EXTRACT) → CashProcessor → RabbitMQ (cash_information_queue) → PostgreSQL (cash_information)
DB2 (ASSET_MASTER) → AssetsProcessor → RabbitMQ (asset_owned_queue) → PostgreSQL (asset_owned)
DB2 (GLI_TRX_EXTRACT) → BotBalancesProcessor → RabbitMQ (balances_bot_queue) → PostgreSQL (balances_bot)
DB2 (GLI_TRX_EXTRACT) → MnosProcessor → RabbitMQ (balances_with_mnos_queue) → PostgreSQL (balances_with_mnos)
DB2 (GLI_TRX_EXTRACT) → OtherBanksProcessor → RabbitMQ (balance_with_other_banks_queue) → PostgreSQL (balance_with_other_bank)
DB2 (GLI_TRX_EXTRACT) → OtherAssetsProcessor → RabbitMQ (other_assets_queue) → PostgreSQL (other_assets)
```

## Dependencies
- DB2: pyodbc connection
- PostgreSQL: psycopg2
- Message Queue: RabbitMQ (pika)
- State Management: Redis
- Data Processing: Threading for parallel consumers