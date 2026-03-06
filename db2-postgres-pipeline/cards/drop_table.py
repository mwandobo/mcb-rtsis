#!/usr/bin/env python3
"""Drop cardInformation table"""

import psycopg2
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

config = Config()
conn = psycopg2.connect(
    host=config.database.pg_host,
    port=config.database.pg_port,
    database=config.database.pg_database,
    user=config.database.pg_user,
    password=config.database.pg_password
)

cursor = conn.cursor()
cursor.execute('DROP TABLE IF EXISTS "cardInformation" CASCADE')
conn.commit()
print("Dropped cardInformation table")
conn.close()
