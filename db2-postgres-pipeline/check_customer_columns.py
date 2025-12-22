#!/usr/bin/env python3
"""
Check CUSTOMER table columns to fix the agents query
"""

from db2_connection import DB2Connection

def check_customer_columns():
    """Check CUSTOMER table structure"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get CUSTOMER table structure
        print("🔍 CUSTOMER Table Columns:")
        cursor.execute("""
            SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS 
            FROM SYSCAT.COLUMNS 
            WHERE TABNAME = 'CUSTOMER' 
            ORDER BY COLNO
        """)
        columns = cursor.fetchall()
        for col in columns:
            print(f"   {col[0]}: {col[1]}({col[2]}) {'NULL' if col[4] == 'Y' else 'NOT NULL'}")
        
        # Test the current agents.sql query to see what's wrong
        print(f"\n🔍 Testing current agents.sql query:")
        try:
            with open('../sqls/agents.sql', 'r') as f:
                query = f.read()
            
            # Remove comments and clean up
            query_lines = [line for line in query.split('\n') if not line.strip().startswith('--')]
            clean_query = '\n'.join(query_lines)
            
            print("   Executing query...")
            cursor.execute(clean_query)
            rows = cursor.fetchall()
            print(f"   Query returned {len(rows)} rows")
            
            if rows:
                print("   Sample results:")
                for i, row in enumerate(rows[:3], 1):
                    print(f"   {i}. Agent ID: {row[2]}, Name: {row[1][:50]}")
                    
        except Exception as e:
            print(f"   Error executing query: {e}")
            
            # Try to identify which columns are causing issues
            print(f"\n🔍 Checking problematic columns:")
            problematic_columns = [
                'ACCOUNT_NUMBER', 'REGION', 'DISTRICT', 'WARD', 
                'HOUSE_NUMBER', 'POSTAL_ADDRESS'
            ]
            
            for col in problematic_columns:
                try:
                    cursor.execute(f"SELECT {col} FROM CUSTOMER FETCH FIRST 1 ROWS ONLY")
                    print(f"   ✅ {col} exists")
                except Exception as col_error:
                    print(f"   ❌ {col} does not exist: {col_error}")

if __name__ == "__main__":
    check_customer_columns()