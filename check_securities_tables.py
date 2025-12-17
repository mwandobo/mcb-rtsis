#!/usr/bin/env python3

import jaydebeapi
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('db2-postgres-pipeline/.env')

# DB2 connection parameters
db2_host = os.getenv('DB2_HOST')
db2_port = os.getenv('DB2_PORT')
db2_database = os.getenv('DB2_DATABASE')
db2_username = os.getenv('DB2_USERNAME')
db2_password = os.getenv('DB2_PASSWORD')

# JDBC URL
jdbc_url = f'jdbc:db2://{db2_host}:{db2_port}/{db2_database}'
jdbc_driver = 'com.ibm.db2.jcc.DB2Driver'
jar_path = 'db2-postgres-pipeline/jdbc-drivers/db2jcc4.jar'

try:
    # Connect to DB2
    conn = jaydebeapi.connect(jdbc_driver, jdbc_url, [db2_username, db2_password], jar_path)
    cursor = conn.cursor()
    
    # Check record counts for potential securities tables
    tables_to_check = [
        'TRBOND',
        'RSKCO_SECURITIES', 
        'GLI_TRX_EXTRACT',
        'SECURITIES',
        'SECURITIES_GL',
        'CUST_SECURITIES',
        'COLLATERAL_TABLE',
        'PRODUCT',
        'DEPOSIT_ACCOUNT',
        'GLG_ACCOUNT'
    ]
    
    print('Table Record Counts:')
    print('=' * 50)
    
    for table in tables_to_check:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM PROFITS.{table}')
            count = cursor.fetchone()[0]
            print(f'{table:<20}: {count:>15,} records')
        except Exception as e:
            print(f'{table:<20}: ERROR - {str(e)[:50]}...')
    
    print('\n' + '=' * 50)
    print('Checking GLI_TRX_EXTRACT with GL account filters:')
    print('=' * 50)
    
    # Check GLI_TRX_EXTRACT with different GL account patterns
    gl_patterns = ['13%', '130%', '131%', '132%', '133%', '134%', '135%']
    
    for pattern in gl_patterns:
        try:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM PROFITS.GLI_TRX_EXTRACT gte
                LEFT JOIN PROFITS.GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
                WHERE gl.EXTERNAL_GLACCOUNT LIKE '{pattern}'
                AND gte.DC_AMOUNT > 0
            """)
            count = cursor.fetchone()[0]
            print(f'GL Account {pattern:<8}: {count:>15,} records')
        except Exception as e:
            print(f'GL Account {pattern:<8}: ERROR - {str(e)[:50]}...')
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f'Connection error: {e}')