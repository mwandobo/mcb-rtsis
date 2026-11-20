#!/usr/bin/env python3
"""DB2 → RabbitMQ → PostgreSQL streaming pipeline for unearned_income.sql."""

import hashlib
import json
import logging
import os
import sys
import threading
import time
from contextlib import contextmanager
from typing import Optional

import pika
import psycopg2
import psycopg2.extras

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from db2_connection import DB2Connection


class UnearnedIncomeStreamingPipeline:
    target_table = '"unearnedIncome"'
    queue_name = 'unearned_income_queue'
    dead_letter_queue = 'unearned_income_dead_letter'
    dead_letter_exchange = 'unearned_income_dlx'

    def __init__(self, batch_size=1000, consumer_batch_size=100):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = consumer_batch_size
        self.producer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        self.total_produced = 0
        self.total_consumed = 0
        self._stats_lock = threading.Lock()
        self._thread_error: Optional[BaseException] = None
        self.logger = logging.getLogger(__name__)

    def get_query(self):
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(root, 'sqls', 'unearned_income.sql'), encoding='utf-8') as file:
            return file.read().strip()

    @contextmanager
    def postgres_connection(self):
        connection = psycopg2.connect(
            host=self.config.database.pg_host, port=self.config.database.pg_port,
            database=self.config.database.pg_database, user=self.config.database.pg_user,
            password=self.config.database.pg_password,
        )
        try:
            yield connection
        finally:
            connection.close()

    def setup_target_table(self):
        with self.postgres_connection() as connection, connection.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS "unearnedIncome" (
                    id BIGSERIAL PRIMARY KEY,
                    "recordKey" VARCHAR(64) UNIQUE,
                    "reportingDate" VARCHAR(12), "unearnedIncomeType" VARCHAR(100), "beneficiaryName" VARCHAR(255),
                    "transactionDate" VARCHAR(12), currency VARCHAR(20),
                    "orgAmount" NUMERIC(31, 2), "usdAmount" NUMERIC(31, 2), "tzsAmount" NUMERIC(31, 2),
                    "sectorSnaClassification" VARCHAR(100)
                )
            ''')
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_unearnedIncome_record_key ON "unearnedIncome" ("recordKey")')
            connection.commit()

    def rabbitmq_connection(self):
        credentials = pika.PlainCredentials(self.config.message_queue.rabbitmq_user, self.config.message_queue.rabbitmq_password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=self.config.message_queue.rabbitmq_host, port=self.config.message_queue.rabbitmq_port,
            credentials=credentials, heartbeat=600, blocked_connection_timeout=300,
        ))
        return connection, connection.channel()

    def setup_queue(self):
        connection, channel = self.rabbitmq_connection()
        try:
            channel.exchange_declare(exchange=self.dead_letter_exchange, exchange_type='direct', durable=True)
            channel.queue_declare(queue=self.dead_letter_queue, durable=True)
            channel.queue_bind(queue=self.dead_letter_queue, exchange=self.dead_letter_exchange, routing_key=self.queue_name)
            channel.queue_declare(queue=self.queue_name, durable=True, arguments={
                'x-dead-letter-exchange': self.dead_letter_exchange,
                'x-dead-letter-routing-key': self.queue_name,
            })
        finally:
            connection.close()

    @staticmethod
    def _text(value):
        return None if value is None else str(value).strip()

    @staticmethod
    def _number(value):
        return None if value is None else float(value)

    def process_record(self, row):
        values = [
            self._text(row[0]), self._text(row[1]), self._text(row[2]), self._text(row[3]),
            self._text(row[4]), self._number(row[5]), self._number(row[6]), self._number(row[7]),
            self._text(row[8]),
        ]
        # Record identity uses GLI_TRX_EXTRACT's own primary key: FK_UNITCODETRXUNIT,
        # FK_USRCODE, LINE_NUM, TRN_DATE, TRN_SNUM (columns 9-11 plus transactionDate
        # at column 3 -- LINE_NUM/TRN_SNUM are sequence numbers that reset daily, so
        # TRN_DATE must be included or postings from different days collide).
        key_values = [
            self._text(row[9]), self._text(row[10]), self._text(row[11]),
            values[3], self._text(row[12]),
        ]
        record_key = hashlib.sha256('|'.join('' if value is None else str(value) for value in key_values).encode('utf-8')).hexdigest()
        return {'values': values, 'recordKey': record_key}

    def producer_thread(self):
        connection = None
        try:
            connection, channel = self.rabbitmq_connection()
            with self.db2_conn.get_connection(log_connection=True) as db2:
                cursor = db2.cursor()
                cursor.execute(self.get_query())
                while rows := cursor.fetchmany(self.batch_size):
                    for row in rows:
                        channel.basic_publish(exchange='', routing_key=self.queue_name, body=json.dumps(self.process_record(row)),
                            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))
                        with self._stats_lock:
                            self.total_produced += 1
        except BaseException as error:
            self._thread_error = error
            self.logger.exception('Unearned-income producer failed')
        finally:
            self.producer_finished.set()
            if connection:
                connection.close()

    def insert_batch(self, records):
        unique = {record['recordKey']: record for record in records}
        values = [(record['recordKey'], *record['values']) for record in unique.values()]
        with self.postgres_connection() as connection, connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, '''
                INSERT INTO "unearnedIncome" (
                    "recordKey", "reportingDate", "unearnedIncomeType", "beneficiaryName", "transactionDate",
                    currency, "orgAmount", "usdAmount", "tzsAmount", "sectorSnaClassification"
                ) VALUES %s
                ON CONFLICT ("recordKey") DO UPDATE SET
                    "reportingDate"=EXCLUDED."reportingDate", "unearnedIncomeType"=EXCLUDED."unearnedIncomeType",
                    "beneficiaryName"=EXCLUDED."beneficiaryName", "transactionDate"=EXCLUDED."transactionDate",
                    currency=EXCLUDED.currency, "orgAmount"=EXCLUDED."orgAmount", "usdAmount"=EXCLUDED."usdAmount",
                    "tzsAmount"=EXCLUDED."tzsAmount", "sectorSnaClassification"=EXCLUDED."sectorSnaClassification"
            ''', values)
            connection.commit()

    def consumer_thread(self):
        connection, channel = self.rabbitmq_connection()
        records, tags, idle = [], [], 0
        try:
            while not self.stop_consumer.is_set():
                method, _, body = channel.basic_get(queue=self.queue_name, auto_ack=False)
                if method is None:
                    idle = idle + 1 if self.producer_finished.is_set() else 0
                    if idle >= 3:
                        break
                    time.sleep(1)
                    continue
                idle = 0
                records.append(json.loads(body.decode('utf-8')))
                tags.append(method.delivery_tag)
                if len(records) >= self.consumer_batch_size:
                    self.insert_batch(records)
                    for tag in tags: channel.basic_ack(tag)
                    with self._stats_lock: self.total_consumed += len(records)
                    records, tags = [], []
            if records:
                self.insert_batch(records)
                for tag in tags: channel.basic_ack(tag)
                with self._stats_lock: self.total_consumed += len(records)
        except BaseException as error:
            self._thread_error = error
            self.logger.exception('Unearned-income consumer failed')
        finally:
            connection.close()

    def run_streaming_pipeline(self):
        self.setup_target_table()
        self.setup_queue()
        consumer = threading.Thread(target=self.consumer_thread, name='unearned-income-consumer')
        producer = threading.Thread(target=self.producer_thread, name='unearned-income-producer')
        consumer.start(); producer.start(); producer.join(); consumer.join(timeout=120)
        if consumer.is_alive():
            self.stop_consumer.set(); consumer.join(timeout=30)
        if consumer.is_alive():
            raise RuntimeError('Unearned-income consumer did not stop')
        if self._thread_error:
            raise RuntimeError('Unearned-income pipeline failed') from self._thread_error
        self.logger.info('Unearned-income pipeline completed: produced=%s, consumed=%s', self.total_produced, self.total_consumed)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    UnearnedIncomeStreamingPipeline().run_streaming_pipeline()
