#!/usr/bin/env python3
"""
Check table structures to understand correct column names
"""

from db2_connection import DB2Connection

def check_table_structures():
    """Check table structures"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Check DEPOSIT_ACCOUNT structure
        print("🔍 DEPOSIT_ACCOUNT Table Structure:")
        try:
            cursor.execute("""
                SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS 
                FROM SYSCAT.COLUMNS 
                WHERE TABNAME = 'DEPOSIT_ACCOUNT' 
                ORDER BY COLNO
            """)
            columns = cursor.fetchall()
            for col in columns:
                print(f"   {col[0]}: {col[1]}({col[2]}) {'NULL' if col[4] == 'Y' else 'NOT NULL'}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Check PRODUCT structure
        print("\n🔍 PRODUCT Table Structure:")
        try:
            cursor.execute("""
                SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS 
                FROM SYSCAT.COLUMNS 
                WHERE TABNAME = 'PRODUCT' 
                ORDER BY COLNO
            """)
            columns = cursor.fetchall()
            for col in columns:
                print(f"   {col[0]}: {col[1]}({col[2]}) {'NULL' if col[4] == 'Y' else 'NOT NULL'}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Check GLI_TRX_EXTRACT structure
        print("\n🔍 GLI_TRX_EXTRACT Table Structure (first 20 columns):")
        try:
            cursor.execute("""
                SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS 
                FROM SYSCAT.COLUMNS 
                WHERE TABNAME = 'GLI_TRX_EXTRACT' 
                ORDER BY COLNO
                FETCH FIRST 20 ROWS ONLY
            """)
            columns = cursor.fetchall()
            for col in columns:
                print(f"   {col[0]}: {col[1]}({col[2]}) {'NULL' if col[4] == 'Y' else 'NOT NULL'}")
        except Exception as e:
            print(f"   Error: {e}")
        
        # Look for any tables that might contain agent information
        print("\n🔍 Looking for tables with 'AGENT' in column names:")
        try:
            cursor.execute("""
                SELECT DISTINCT TABNAME, COLNAME
                FROM SYSCAT.COLUMNS 
                WHERE TABSCHEMA = 'PROFITS'
                  AND UPPER(COLNAME) LIKE '%AGENT%'
                ORDER BY TABNAME, COLNAME
            """)
            agent_columns = cursor.fetchall()
            
            current_table = None
            for table, column in agent_columns:
                if table != current_table:
                    print(f"\n   Table: {table}")
                    current_table = table
                print(f"     - {column}")
                
        except Exception as e:
            print(f"   Error: {e}")
        
        # Check for any mobile money or agent service related tables
        print("\n🔍 Tables that might contain mobile money/agent data:")
        tables_to_check = [
            'DEPOSIT_ACCOUNT',
            'PRODUCT', 
            'ACCOUNT_BALANCE',
            'TRANSACTION_LOG',
            'SERVICE_REQUEST',
            'MOBILE_BANKING',
            'ELECTRONIC_BANKING'
        ]
        
        for table in tables_to_check:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"   {table}: {count:,} records")
            except Exception as e:
                print(f"   {table}: Table not found or error - {e}")

if __name__ == "__main__":
    check_table_structures()