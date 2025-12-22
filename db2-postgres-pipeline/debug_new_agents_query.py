#!/usr/bin/env python3
"""
Debug the new agents query to understand why we're getting duplicates
"""

from db2_connection import DB2Connection

def debug_agents_query():
    """Debug the agents query"""
    db2_conn = DB2Connection()
    
    with db2_conn.get_connection() as conn:
        cursor = conn.cursor()
        
        # Check the current query results
        print("🔍 Current Agents Query Results:")
        cursor.execute("""
            SELECT 
                c.CUST_ID,
                TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) AS agentName,
                c.MOBILE_TEL,
                c.CUST_TYPE,
                c.ENTRY_STATUS
            FROM CUSTOMER c
            WHERE c.CUST_TYPE = '2'  -- Business customers
                AND c.ENTRY_STATUS = '1'  -- Active customers
                AND c.MOBILE_TEL IS NOT NULL 
                AND c.MOBILE_TEL != ''
                AND LENGTH(TRIM(c.MOBILE_TEL)) > 5  -- Valid mobile numbers
                AND COALESCE(c.LAST_UPDATE, c.CUSTOMER_BEGIN_DAT) >= TIMESTAMP('2016-01-01 00:00:00')
            ORDER BY c.CUST_ID
        """)
        
        rows = cursor.fetchall()
        print(f"   Total business customers with mobile: {len(rows)}")
        
        # Show unique customers
        unique_customers = {}
        for row in rows:
            cust_id = row[0]
            if cust_id not in unique_customers:
                unique_customers[cust_id] = row
        
        print(f"   Unique customers: {len(unique_customers)}")
        print("   Sample unique customers:")
        for i, (cust_id, row) in enumerate(list(unique_customers.items())[:10], 1):
            print(f"   {i}. ID: {row[0]}, Name: {row[1][:50]}, Mobile: {row[2]}, Type: {row[3]}")
        
        # Check why we might be getting duplicates in the original query
        print(f"\n🔍 Checking for potential duplicate sources:")
        
        # Check if there are multiple records per customer in any related tables
        cursor.execute("""
            SELECT COUNT(*) as total_records, COUNT(DISTINCT c.CUST_ID) as unique_customers
            FROM CUSTOMER c
            WHERE c.CUST_TYPE = '2'  
                AND c.ENTRY_STATUS = '1'  
                AND c.MOBILE_TEL IS NOT NULL 
                AND c.MOBILE_TEL != ''
                AND LENGTH(TRIM(c.MOBILE_TEL)) > 5
        """)
        result = cursor.fetchone()
        print(f"   Total records: {result[0]}, Unique customers: {result[1]}")
        
        if result[0] > result[1]:
            print("   ⚠️  There are duplicate customer records!")
            
            # Find duplicates
            cursor.execute("""
                SELECT c.CUST_ID, COUNT(*) as count
                FROM CUSTOMER c
                WHERE c.CUST_TYPE = '2'  
                    AND c.ENTRY_STATUS = '1'  
                    AND c.MOBILE_TEL IS NOT NULL 
                    AND c.MOBILE_TEL != ''
                    AND LENGTH(TRIM(c.MOBILE_TEL)) > 5
                GROUP BY c.CUST_ID
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall()
            print(f"   Customers with duplicates: {len(duplicates)}")
            for dup in duplicates[:5]:
                print(f"     Customer {dup[0]}: {dup[1]} records")

if __name__ == "__main__":
    debug_agents_query()