#!/usr/bin/env python3
"""
Test the v2 query to see how many fields it returns
"""

from db2_connection import DB2Connection

def test_v2_query():
    """Test the v2 query to count fields"""
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Simple test query to get field count
            test_query = """
            WITH LatestInstallments AS (SELECT *
                                        FROM (SELECT li.*,
                                                     ROW_NUMBER() OVER (PARTITION BY li.ACC_SN ORDER BY li.INSTALL_SN DESC) AS rn
                                              FROM LOAN_INSTALLMENTS li) t
                                        WHERE t.rn = 1),
                 ProfitsAccount AS (SELECT CUST_ID,
                                           MIN(ACCOUNT_NUMBER) AS ACCOUNT_NUMBER
                                    FROM PROFITS_ACCOUNT
                                    GROUP BY CUST_ID),
                 MainQuery AS (SELECT CURRENT_TIMESTAMP AS reportingDate,
                                      LTRIM(RTRIM(cust.CUST_ID)) AS customerIdentificationNumber,
                                      LTRIM(RTRIM(pa.ACCOUNT_NUMBER)) AS accountNumber,
                                      LTRIM(RTRIM(cust.FIRST_NAME)) AS clientName
                               FROM LOAN_ACCOUNT la
                                        LEFT JOIN ProfitsAccount pa ON pa.CUST_ID = la.CUST_ID
                                        LEFT JOIN CUSTOMER cust ON la.CUST_ID = cust.CUST_ID)
            SELECT *
            FROM MainQuery
            FETCH FIRST 1 ROWS ONLY
            """
            
            cursor.execute(test_query)
            row = cursor.fetchone()
            
            if row:
                print(f"Test query returned {len(row)} fields")
                for i, value in enumerate(row):
                    print(f"  Field {i}: {value}")
            else:
                print("No data returned")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_v2_query()