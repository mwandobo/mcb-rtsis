#!/usr/bin/env python3
"""Read-only source/target diagnosis for corporate customer 9999902."""

import os
import sys

import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection


CUSTOMER_IDS = (9999901, 9999902)


def main():
    with DB2Connection().get_connection(log_connection=True) as connection:
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT CUST_ID, CUST_TYPE_IND, ID_NO, CUSTOMER_BEGIN_DAT
            FROM W_DIM_CUSTOMER
            WHERE CUST_ID IN (?, ?)
            ORDER BY CUSTOMER_BEGIN_DAT DESC
            """,
            CUSTOMER_IDS,
        )
        print("DB2 W_DIM_CUSTOMER")
        for row in cursor.fetchall():
            print(f"  {row}")

        cursor.execute(
            """
            SELECT CUST_ID, SURNAME, CUST_TYPE, CUST_OPEN_DATE
            FROM CUSTOMER
            WHERE CUST_ID IN (?, ?)
            ORDER BY CUST_ID
            """,
            CUSTOMER_IDS,
        )
        print("DB2 CUSTOMER")
        for row in cursor.fetchall():
            print(f"  {row}")

        cursor.execute(
            """
            SELECT
                COUNT(*) AS active_address_rows,
                SUM(CASE WHEN communication_addr = '1' AND entry_status = '1' THEN 1 ELSE 0 END)
                    AS primary_active_address_rows
            FROM CUST_ADDRESS
            WHERE FK_CUSTOMERCUST_ID IN (?, ?)
            """,
            CUSTOMER_IDS,
        )
        print(f"DB2 address rows: {cursor.fetchone()}")

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM AGREEMENT a
            JOIN CUST_ADDRESS ca
              ON a.FK_CUST_ADDRESSFK = ca.FK_CUSTOMERCUST_ID
             AND a.FK_CUST_ADDRESSSER = ca.SERIAL_NUM
            WHERE ca.FK_CUSTOMERCUST_ID IN (?, ?)
            """,
            CUSTOMER_IDS,
        )
        print(f"DB2 agreement/address matches: {cursor.fetchone()[0]}")

        cursor.execute(
            """
            SELECT TRIM(ACCOUNT_NUMBER), CUST_ID, ACCOUNT_SER_NUM, PRODUCT_ID
            FROM PROFITS_ACCOUNT
            WHERE TRIM(ACCOUNT_NUMBER) IN ('9999010002', '9999020001')
            ORDER BY ACCOUNT_NUMBER
            """
        )
        print("DB2 account-to-customer links")
        for row in cursor.fetchall():
            print(f"  {row}")

    config = Config()
    with psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                SELECT "customerIdentificationNumber", "companyName", created_at
                FROM "personalDataCorporates"
                WHERE "customerIdentificationNumber" = %s
                ''',
                (str(CUSTOMER_IDS[0]),),
            )
            print("PostgreSQL personalDataCorporates")
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    print(f"  {row}")
            else:
                print("  No matching row")


if __name__ == "__main__":
    main()
