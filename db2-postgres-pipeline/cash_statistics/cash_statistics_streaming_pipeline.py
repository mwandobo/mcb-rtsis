#!/usr/bin/env python3
"""DB2 -> RabbitMQ -> PostgreSQL pipeline for cash-statistics.sql."""
import hashlib
import json
import logging
import os
import sys
import threading
import time
from contextlib import contextmanager

import pika
import psycopg2
import psycopg2.extras

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from db2_connection import DB2Connection


class CashStatisticsStreamingPipeline:
    target_table = '"cashStatistics"'
    queue_name = 'cash_statistics_queue'
    dead_letter_queue = 'cash_statistics_dead_letter'
    dead_letter_exchange = 'cash_statistics_dlx'

    def __init__(self, batch_size=1000, consumer_batch_size=100):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = consumer_batch_size
        self.producer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        self.total_produced = self.total_consumed = 0
        self._stats_lock = threading.Lock()
        self._thread_error = None
        self.logger = logging.getLogger(__name__)

    def get_query(self):
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(root, 'sqls', 'cash-statistics.sql'), encoding='utf-8') as file:
            return file.read().strip()

    @contextmanager
    def postgres_connection(self):
        c = self.config.database
        conn = psycopg2.connect(host=c.pg_host, port=c.pg_port, dbname=c.pg_database,
                                user=c.pg_user, password=c.pg_password)
        try:
            yield conn
        finally:
            conn.close()

    def setup_target_table(self):
        with self.postgres_connection() as conn, conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS "cashStatistics" (
                id BIGSERIAL PRIMARY KEY,
                "recordKey" VARCHAR(64) NOT NULL UNIQUE,
                "reportingDate" VARCHAR(12) NOT NULL,
                "branchCode" VARCHAR(50) NOT NULL,
                "cashCategory" VARCHAR(100) NOT NULL,
                "cashSubCategory" VARCHAR(100),
                currency VARCHAR(5) NOT NULL,
                "cashDenomination" NUMERIC(18,2) NOT NULL,
                "quantityOfCoinsNotes" NUMERIC(24,0) NOT NULL,
                "orgAmount" NUMERIC(24,2) NOT NULL,
                "usdAmount" NUMERIC(24,2),
                "tzsAmount" NUMERIC(24,2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_cashStatistics_branch ON "cashStatistics"("branchCode")')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_cashStatistics_currency ON "cashStatistics"(currency)')
            conn.commit()

    def rabbitmq_connection(self):
        q = self.config.message_queue
        conn = pika.BlockingConnection(pika.ConnectionParameters(
            host=q.rabbitmq_host, port=q.rabbitmq_port,
            credentials=pika.PlainCredentials(q.rabbitmq_user, q.rabbitmq_password),
            heartbeat=600, blocked_connection_timeout=300))
        return conn, conn.channel()

    def setup_queue(self):
        conn, ch = self.rabbitmq_connection()
        try:
            ch.exchange_declare(exchange=self.dead_letter_exchange, exchange_type='direct', durable=True)
            ch.queue_declare(queue=self.dead_letter_queue, durable=True)
            ch.queue_bind(queue=self.dead_letter_queue, exchange=self.dead_letter_exchange,
                          routing_key=self.queue_name)
            ch.queue_declare(queue=self.queue_name, durable=True, arguments={
                'x-dead-letter-exchange': self.dead_letter_exchange,
                'x-dead-letter-routing-key': self.queue_name})
        finally:
            conn.close()

    @staticmethod
    def text(value):
        return None if value is None else str(value).strip()

    @staticmethod
    def number(value):
        return None if value is None else float(value)

    def process_record(self, row):
        values = [self.text(row[index]) if index in (0, 1, 2, 3, 4) else self.number(row[index])
                  for index in range(10)]
        key_fields = (values[1], values[2], values[3], values[4], values[5])
        record_key = hashlib.sha256('|'.join(str(value or '') for value in key_fields).encode()).hexdigest()
        return {'recordKey': record_key, 'values': values}

    def producer_thread(self):
        conn = None
        try:
            conn, ch = self.rabbitmq_connection()
            with self.db2_conn.get_connection(log_connection=True) as db2:
                cur = db2.cursor()
                cur.execute(self.get_query())
                while rows := cur.fetchmany(self.batch_size):
                    for row in rows:
                        ch.basic_publish(exchange='', routing_key=self.queue_name,
                            body=json.dumps(self.process_record(row)),
                            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))
                        with self._stats_lock:
                            self.total_produced += 1
        except BaseException as error:
            self._thread_error = error
            self.logger.exception('Cash statistics producer failed')
        finally:
            self.producer_finished.set()
            if conn:
                conn.close()

    def insert_batch(self, records):
        unique = {record['recordKey']: record for record in records}
        rows = [(record['recordKey'], *record['values']) for record in unique.values()]
        with self.postgres_connection() as conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, '''INSERT INTO "cashStatistics" (
                "recordKey", "reportingDate", "branchCode", "cashCategory", "cashSubCategory",
                currency, "cashDenomination", "quantityOfCoinsNotes", "orgAmount", "usdAmount", "tzsAmount")
                VALUES %s
                ON CONFLICT ("recordKey") DO UPDATE SET
                    "reportingDate"=EXCLUDED."reportingDate",
                    "quantityOfCoinsNotes"=EXCLUDED."quantityOfCoinsNotes",
                    "orgAmount"=EXCLUDED."orgAmount", "usdAmount"=EXCLUDED."usdAmount",
                    "tzsAmount"=EXCLUDED."tzsAmount", updated_at=CURRENT_TIMESTAMP''', rows)
            conn.commit()

    def consumer_thread(self):
        conn, ch = self.rabbitmq_connection()
        records, tags, idle = [], [], 0
        try:
            while not self.stop_consumer.is_set():
                method, _, body = ch.basic_get(queue=self.queue_name, auto_ack=False)
                if method is None:
                    idle = idle + 1 if self.producer_finished.is_set() else 0
                    if idle >= 3:
                        break
                    time.sleep(1)
                    continue
                idle = 0
                records.append(json.loads(body.decode()))
                tags.append(method.delivery_tag)
                if len(records) >= self.consumer_batch_size:
                    self.insert_batch(records)
                    for tag in tags:
                        ch.basic_ack(tag)
                    with self._stats_lock:
                        self.total_consumed += len(records)
                    records, tags = [], []
            if records:
                self.insert_batch(records)
                for tag in tags:
                    ch.basic_ack(tag)
                with self._stats_lock:
                    self.total_consumed += len(records)
        except BaseException as error:
            self._thread_error = error
            self.logger.exception('Cash statistics consumer failed')
        finally:
            conn.close()

    def run_streaming_pipeline(self):
        self.setup_target_table()
        self.setup_queue()
        consumer = threading.Thread(target=self.consumer_thread)
        producer = threading.Thread(target=self.producer_thread)
        consumer.start()
        producer.start()
        producer.join()
        consumer.join(timeout=90)
        if consumer.is_alive():
            self.stop_consumer.set()
            consumer.join(timeout=30)
        if consumer.is_alive():
            raise RuntimeError('Cash statistics consumer did not stop')
        if self._thread_error:
            raise RuntimeError('Cash statistics pipeline failed') from self._thread_error
        self.logger.info('Cash statistics pipeline completed: produced=%s consumed=%s',
                         self.total_produced, self.total_consumed)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    CashStatisticsStreamingPipeline().run_streaming_pipeline()
