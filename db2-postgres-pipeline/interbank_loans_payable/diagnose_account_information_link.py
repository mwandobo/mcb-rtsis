#!/usr/bin/env python3
"""Read-only reconciliation for interbank payable account mappings."""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from config import Config
from db2_connection import DB2Connection


SUMMARY_SQL = """
WITH payable AS (
    SELECT mm.DEAL_NO, mm.FK_CORRESP_DEP_ACC
    FROM TREASURY_MM_DEAL mm
    WHERE mm.DEAL_OPERATION = 'B'
      AND mm.STATUS <> 'C'
), linked AS (
    SELECT p.DEAL_NO, pa.ACCOUNT_NUMBER
    FROM payable p
    LEFT JOIN PROFITS_ACCOUNT pa
      ON pa.ACCOUNT_SER_NUM = p.FK_CORRESP_DEP_ACC
)
SELECT
    COUNT(*) AS payable_deals,
    COUNT(DISTINCT DEAL_NO) AS distinct_deal_numbers,
    SUM(CASE WHEN ACCOUNT_NUMBER IS NOT NULL
              AND LENGTH(TRIM(ACCOUNT_NUMBER)) > 0 THEN 1 ELSE 0 END)
        AS deals_with_linked_account,
    COUNT(DISTINCT CASE WHEN ACCOUNT_NUMBER IS NOT NULL
                         AND LENGTH(TRIM(ACCOUNT_NUMBER)) > 0
                    THEN TRIM(ACCOUNT_NUMBER) END)
        AS distinct_linked_accounts
FROM linked
"""

LINKED_ACCOUNTS_SQL = """
SELECT
    pa.PRODUCT_ID,
    TRIM(pa.ACCOUNT_NUMBER) AS account_number,
    COUNT(*) AS payable_deal_count
FROM TREASURY_MM_DEAL mm
JOIN PROFITS_ACCOUNT pa
  ON pa.ACCOUNT_SER_NUM = mm.FK_CORRESP_DEP_ACC
WHERE mm.DEAL_OPERATION = 'B'
  AND mm.STATUS <> 'C'
GROUP BY pa.PRODUCT_ID, TRIM(pa.ACCOUNT_NUMBER)
ORDER BY payable_deal_count DESC
"""

ACCOUNT_INFORMATION_ELIGIBILITY_SQL = """
SELECT DISTINCT
    TRIM(pa.ACCOUNT_NUMBER) AS account_number,
    pa.PRODUCT_ID,
    wp.TREE_LEVEL_1,
    wp.TREE_LEVEL_2
FROM PROFITS_ACCOUNT pa
LEFT JOIN W_DIM_PRODUCT wp
  ON wp.PRODUCT_CODE = pa.PRODUCT_ID
WHERE TRIM(pa.ACCOUNT_NUMBER) <> 'DUMMY TRS-LG'
  AND TRIM(pa.ACCOUNT_NUMBER) <> ''
  AND pa.PRFT_SYSTEM <> 19
  AND (
      (
          pa.PRODUCT_ID NOT IN (38220, 38801)
          AND wp.TREE_LEVEL_2 NOT IN ('DUMMY', 'HIDDEN ACCOUNT')
          AND wp.TREE_LEVEL_1 NOT IN ('Agreement', 'Service', 'Collateral')
      )
      OR (
          pa.PRODUCT_ID = 38801
          AND EXISTS (
              SELECT 1
              FROM TREASURY_MM_DEAL mm
              WHERE mm.FK_CORRESP_DEP_ACC = pa.ACCOUNT_SER_NUM
                AND mm.DEAL_OPERATION = 'B'
                AND mm.STATUS <> 'C'
          )
      )
  )
  AND TRIM(pa.ACCOUNT_NUMBER) IN ('9999020001', '9999010002')
ORDER BY account_number
"""


def main():
    with DB2Connection().get_connection(log_connection=True) as connection:
        cursor = connection.cursor()

        cursor.execute(SUMMARY_SQL)
        total, distinct_deals, linked_deals, linked_accounts = cursor.fetchone()
        print("Payable deal reconciliation")
        print(f"  Payable rows: {total}")
        print(f"  Distinct DEAL_NO values: {distinct_deals}")
        print(f"  Rows linked to PROFITS_ACCOUNT: {linked_deals}")
        print(f"  Distinct linked accounts: {linked_accounts}")

        cursor.execute(LINKED_ACCOUNTS_SQL)
        print("\nLinked account distribution")
        for product_id, account_number, deal_count in cursor.fetchall():
            print(f"  Product {product_id} | {account_number} | {deal_count} payable rows")

        cursor.execute(ACCOUNT_INFORMATION_ELIGIBILITY_SQL)
        print("\nAccount Information eligibility after the SQL change")
        for account_number, product_id, tree_level_1, tree_level_2 in cursor.fetchall():
            print(
                f"  {account_number} | product {product_id} | "
                f"{tree_level_1} | {tree_level_2}"
            )

    config = Config()
    with psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM "accountInformation"')
            print(f"\nPostgreSQL Account Information rows: {cursor.fetchone()[0]}")
            cursor.execute(
                '''
                SELECT "accountNumber", "accountProductCode"
                FROM "accountInformation"
                WHERE "accountNumber" IN ('9999010002', '9999020001', '9999020002')
                ORDER BY "accountNumber"
                '''
            )
            print("Required payable-linked accounts in PostgreSQL")
            for account_number, product_code in cursor.fetchall():
                print(f"  {account_number} | product {product_code}")

            cursor.execute(
                '''
                SELECT
                    COUNT(*) AS payable_rows,
                    COUNT(DISTINCT "dealNumber") AS distinct_deals,
                    COUNT(DISTINCT "accountNumber") AS distinct_accounts
                FROM "interbankLoansPayable"
                '''
            )
            payable_rows, distinct_deals, distinct_accounts = cursor.fetchone()
            print("\nPostgreSQL Interbank Loans Payable rows")
            print(
                f"  Rows: {payable_rows} | Deals: {distinct_deals} | "
                f"Accounts: {distinct_accounts}"
            )

            cursor.execute(
                '''
                SELECT COUNT(*)
                FROM "interbankLoansPayable" payable
                LEFT JOIN "accountInformation" account_info
                  ON BTRIM(account_info."accountNumber") = BTRIM(payable."accountNumber")
                WHERE COALESCE(BTRIM(payable."accountNumber"), '') <> ''
                  AND account_info."accountNumber" IS NULL
                '''
            )
            print(
                "  Payable accounts missing from Account Information: "
                f"{cursor.fetchone()[0]}"
            )


if __name__ == "__main__":
    main()
