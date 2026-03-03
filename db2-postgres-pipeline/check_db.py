import psycopg2

conn = psycopg2.connect(host='172.10.5.23', port=5432, database='rtsis', user='dv_mcb', password='MCB@123')
cur = conn.cursor()

# Truncate and reset
print("Truncating cardTransactions table...")
cur.execute('TRUNCATE TABLE "cardTransactions" RESTART IDENTITY')
conn.commit()
cur.execute('SELECT COUNT(*) FROM "cardTransactions"')
print(f"Records after truncate: {cur.fetchone()[0]}")

# Clear RabbitMQ queue
try:
    import pika
    creds = pika.PlainCredentials('guest', 'guest')
    params = pika.ConnectionParameters(host='localhost', port=5672, credentials=creds)
    rmq = pika.BlockingConnection(params)
    ch = rmq.channel()
    ch.queue_purge(queue='card_transactions_queue')
    print("RabbitMQ queue purged")
    rmq.close()
except Exception as e:
    print(f"RabbitMQ purge failed: {e}")

print("Ready for fresh pipeline run")
conn.close()
