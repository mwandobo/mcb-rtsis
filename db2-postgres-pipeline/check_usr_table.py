#!/usr/bin/env python3
"""
Check USR table for agent information
"""

from db2_connection import DB2Connection

def check_usr_table():
    """Check USR table structure and data"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Check if USR table exists and get its structure
        print("🔍 USR Table Structure:")
        try:
            cursor.execute("""
                SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS 
                FROM SYSCAT.COLUMNS 
                WHERE TABNAME = 'USR' 
                ORDER BY COLNO
            """)
            columns = cursor.fetchall()
            
            if columns:
                print(f"   Found {len(columns)} columns:")
                for col in columns:
                    print(f"   {col[0]}: {col[1]}({col[2]}) {'NULL' if col[4] == 'Y' else 'NOT NULL'}")
                
                # Get record count
                cursor.execute("SELECT COUNT(*) FROM USR")
                count = cursor.fetchone()[0]
                print(f"\n   Total records: {count:,}")
                
                # Get sample data
                if count > 0:
                    cursor.execute("SELECT * FROM USR FETCH FIRST 10 ROWS ONLY")
                    rows = cursor.fetchall()
                    print(f"\n   Sample data (first 10 records):")
                    for i, row in enumerate(rows, 1):
                        print(f"   {i}. {row}")
                
                # Look for agent-related data in USR
                print(f"\n🔍 Looking for agent-related data in USR:")
                
                # Check if there are any columns that might contain agent info
                agent_columns = [col[0] for col in columns if 'AGENT' in col[0].upper() or 'WAKALA' in col[0].upper() or 'DUKA' in col[0].upper()]
                if agent_columns:
                    print(f"   Agent-related columns found: {agent_columns}")
                else:
                    print(f"   No obvious agent-related columns found")
                
                # Check if USR contains user codes that might be agent codes
                if count > 0:
                    # Look for patterns in user codes or names
                    cursor.execute("""
                        SELECT * FROM USR 
                        WHERE UPPER(USR_NAME) LIKE '%AGENT%'
                           OR UPPER(USR_NAME) LIKE '%WAKALA%'
                           OR UPPER(USR_NAME) LIKE '%DUKA%'
                           OR UPPER(USR_DESCRIPTION) LIKE '%AGENT%'
                           OR UPPER(USR_DESCRIPTION) LIKE '%WAKALA%'
                           OR UPPER(USR_DESCRIPTION) LIKE '%DUKA%'
                        FETCH FIRST 20 ROWS ONLY
                    """)
                    agent_users = cursor.fetchall()
                    
                    if agent_users:
                        print(f"   Found {len(agent_users)} agent-related users:")
                        for i, user in enumerate(agent_users, 1):
                            print(f"   {i}. {user}")
                    else:
                        print(f"   No agent-related users found in USR")
                
            else:
                print("   USR table not found or has no columns")
                
        except Exception as e:
            print(f"   Error accessing USR table: {e}")
        
        # Also check for any tables with USR in the name
        print(f"\n🔍 Tables with 'USR' in name:")
        try:
            cursor.execute("""
                SELECT TABNAME FROM SYSCAT.TABLES 
                WHERE TABSCHEMA = 'PROFITS' 
                  AND TABNAME LIKE '%USR%'
                ORDER BY TABNAME
            """)
            usr_tables = cursor.fetchall()
            
            for table in usr_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    count = cursor.fetchone()[0]
                    print(f"   {table[0]}: {count:,} records")
                except Exception as e:
                    print(f"   {table[0]}: Error - {e}")
                    
        except Exception as e:
            print(f"   Error finding USR tables: {e}")

if __name__ == "__main__":
    check_usr_table()