import psycopg2
from config import Config

c = Config()
conn = psycopg2.connect(host=c.database.pg_host, port=c.database.pg_port, database=c.database.pg_database, user=c.database.pg_user, password=c.database.pg_password)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM "personalData"')
print(f'Count: {cur.fetchone()[0]}')
conn.close()
