#!/usr/bin/env python3
from db2_connection import DB2Connection

db = DB2Connection()
with db.get_connection() as conn:
    cur = conn.cursor()
    try:
        cur.execute('SELECT COUNT(*) FROM INCOME_STATEMENT_GL_LOOKUP')
        print(f'✅ Table exists with {cur.fetchone()[0]} rows')
    except Exception as e:
        print(f'❌ Table does not exist: {e}')
