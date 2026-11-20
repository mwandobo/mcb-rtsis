#!/usr/bin/env python3
"""Read-only counts for the correspondent-customer reporting population."""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db2_connection import DB2Connection


COUNTS_SQL = """
WITH correspondents AS (
    SELECT DISTINCT CUST_ID
    FROM W_DIM_CUSTOMER
    WHERE CUST_TYPE_IND = 'Correspodent'
), correspondent_agreements AS (
    SELECT DISTINCT cc.CUST_ID
    FROM correspondents cc
    JOIN CUST_ADDRESS ca
      ON ca.FK_CUSTOMERCUST_ID = cc.CUST_ID
    JOIN AGREEMENT a
      ON a.FK_CUST_ADDRESSFK = ca.FK_CUSTOMERCUST_ID
     AND a.FK_CUST_ADDRESSSER = ca.SERIAL_NUM
), correspondent_accounts AS (
    SELECT DISTINCT pa.CUST_ID
    FROM correspondents cc
    JOIN PROFITS_ACCOUNT pa
      ON pa.CUST_ID = cc.CUST_ID
    WHERE TRIM(pa.ACCOUNT_NUMBER) <> ''
), payable_correspondents AS (
    SELECT DISTINCT pa.CUST_ID
    FROM TREASURY_MM_DEAL mm
    JOIN PROFITS_ACCOUNT pa
      ON pa.ACCOUNT_SER_NUM = mm.FK_CORRESP_DEP_ACC
    JOIN correspondents cc
      ON cc.CUST_ID = pa.CUST_ID
    WHERE mm.DEAL_OPERATION = 'B'
      AND mm.STATUS <> 'C'
)
SELECT
    (SELECT COUNT(*) FROM correspondents) AS all_correspondents,
    (SELECT COUNT(*) FROM correspondent_agreements) AS correspondents_with_agreement,
    (SELECT COUNT(*) FROM correspondent_accounts) AS correspondents_with_accounts,
    (SELECT COUNT(*) FROM payable_correspondents) AS correspondents_linked_to_payable
FROM SYSIBM.SYSDUMMY1
"""


def main():
    with DB2Connection().get_connection(log_connection=True) as connection:
        cursor = connection.cursor()
        cursor.execute(COUNTS_SQL)
        all_rows, with_agreement, with_accounts, linked_to_payable = cursor.fetchone()
        print(f"All Correspodent customers: {all_rows}")
        print(f"With an agreement/address match: {with_agreement}")
        print(f"With a nonblank PROFITS_ACCOUNT: {with_accounts}")
        print(f"Linked to active interbank payable deals: {linked_to_payable}")


if __name__ == "__main__":
    main()
