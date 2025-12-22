#!/usr/bin/env python3
"""
USR table investigation
"""

from db2_connection import DB2Connection

def investigate_usr():
    """Investigate USR table"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        print("🔍 USR Table Investigation:")
        
        # Get USR structure
        cursor.execute("""
            SELECT COLNAME, TYPENAME, LENGTH 
            FROM SYSCAT.COLUMNS 
            WHERE TABNAME = 'USR' 
            ORDER BY COLNO
        """)
        columns = cursor.fetchall()
        print(f"   Columns ({len(columns)}):")
        for col in columns:
            print(f"     {col[0]}: {col[1]}({col[2]})")
        
        # Get count
        cursor.execute("SELECT COUNT(*) FROM USR")
        count = cursor.fetchone()[0]
        print(f"\n   Total records: {count:,}")
        
        # Get sample data
        cursor.execute("SELECT * FROM USR FETCH FIRST 5 ROWS ONLY")
        rows = cursor.fetchall()
        print(f"\n   Sample data:")
        for i, row in enumerate(rows, 1):
            print(f"     {i}. {row}")
        
        # Look for agent-related users
        cursor.execute("""
            SELECT * FROM USR 
            WHERE UPPER(USR_CODE) LIKE '%AGENT%'
               OR UPPER(USR_NAME) LIKE '%AGENT%'
               OR UPPER(USR_CODE) LIKE '%WAKALA%'
               OR UPPER(USR_NAME) LIKE '%WAKALA%'
            FETCH FIRST 10 ROWS ONLY
        """)
        agent_users = cursor.fetchall()
        
        if agent_users:
            print(f"\n   Agent-related users found: {len(agent_users)}")
            for i, user in enumerate(agent_users, 1):
                print(f"     {i}. {user}")
        else:
            print(f"\n   No agent-related users found")

if __name__ == "__main__":
    investigate_usr()