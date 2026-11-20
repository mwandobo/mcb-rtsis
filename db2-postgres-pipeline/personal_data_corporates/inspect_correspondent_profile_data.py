#!/usr/bin/env python3
"""Read-only check of source completeness for payable-linked correspondents."""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db2_connection import DB2Connection


SQL = """
WITH payable_customers AS (
    SELECT DISTINCT pa.CUST_ID
    FROM TREASURY_MM_DEAL mm
    JOIN PROFITS_ACCOUNT pa
      ON pa.ACCOUNT_SER_NUM = mm.FK_CORRESP_DEP_ACC
    WHERE mm.DEAL_OPERATION = 'B'
      AND mm.STATUS <> 'C'
), latest_dimension AS (
    SELECT CUST_ID, CUST_TYPE_IND, ID_NO, CUSTOMER_BEGIN_DAT
    FROM (
        SELECT
            wdc.*,
            ROW_NUMBER() OVER (
                PARTITION BY CUST_ID
                ORDER BY CUSTOMER_BEGIN_DAT DESC
            ) AS rn
        FROM W_DIM_CUSTOMER wdc
    ) ranked
    WHERE rn = 1
)
SELECT
    c.CUST_ID,
    TRIM(c.SURNAME) AS company_name,
    c.CUST_OPEN_DATE,
    c.NO_OF_EMPLOYEES,
    d.CUST_TYPE_IND,
    TRIM(d.ID_NO) AS dimension_id_number,
    d.CUSTOMER_BEGIN_DAT,
    TRIM(a.ADDRESS_1) AS address_1,
    TRIM(a.CITY) AS city,
    TRIM(a.REGION) AS region,
    TRIM(a.ZIP_CODE) AS zip_code,
    TRIM(a.TELEPHONE) AS telephone
FROM payable_customers pc
JOIN CUSTOMER c
  ON c.CUST_ID = pc.CUST_ID
LEFT JOIN latest_dimension d
  ON d.CUST_ID = c.CUST_ID
LEFT JOIN CUST_ADDRESS a
  ON a.FK_CUSTOMERCUST_ID = c.CUST_ID
 AND a.COMMUNICATION_ADDR = '1'
 AND a.ENTRY_STATUS = '1'
ORDER BY c.CUST_ID
"""


def main():
    with DB2Connection().get_connection(log_connection=True) as connection:
        cursor = connection.cursor()
        cursor.execute(SQL)
        for row in cursor.fetchall():
            print(row)


if __name__ == "__main__":
    main()
