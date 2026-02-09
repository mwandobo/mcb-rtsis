import psycopg2
from config import Config

c = Config()
conn = psycopg2.connect(host=c.database.pg_host, port=c.database.pg_port, database=c.database.pg_database, user=c.database.pg_user, password=c.database.pg_password)
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM "loanInformation"')
print(f'Records: {cursor.fetchone()[0]:,}')
conn.close()