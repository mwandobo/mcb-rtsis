#!/usr/bin/env python3
"""DB2 → RabbitMQ → PostgreSQL streaming pipeline for premises_furniture_equipment.sql."""

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


class PremisesFurnitureEquipmentStreamingPipeline:
    target_table = '"premisesFurnitureEquipment"'
    queue_name = 'premises_furniture_equipment_queue'
    dead_letter_queue = 'premises_furniture_equipment_dead_letter'
    dead_letter_exchange = 'premises_furniture_equipment_dlx'

    COLUMNS = [
        'reportingDate', 'assetCategory', 'usagePremisesFurnitureEquipment', 'acquisitionDate', 'assetType',
        'currency', 'orgAmount', 'usdAmount', 'tzsAmount', 'disposalDate', 'orgAssetDisposedValue',
        'usdAssetDisposedValue', 'tzsAssetDisposedValue', 'orgDepreciation', 'usdDepreciation',
        'tzsDepreciation', 'orgAccumDepreciation', 'usdAccumDepreciation', 'tzsAccumDepreciation',
        'orgImpairmentAmount', 'usdImpairmentAmount', 'tzsImpairmentAmount', 'orgNetBookValue',
        'usdNetBookValue', 'tzsNetBookValue',
    ]

    TEXT_COLUMNS = {
        'reportingDate', 'assetCategory', 'usagePremisesFurnitureEquipment', 'acquisitionDate', 'assetType',
        'currency', 'disposalDate',
    }

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
        with open(os.path.join(root, 'sqls', 'premises_furniture_equipment.sql'), encoding='utf-8') as file:
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
        column_defs = ',\n                    '.join(
            f'"{name}" VARCHAR(255)' if name in self.TEXT_COLUMNS else f'"{name}" NUMERIC(31, 6)'
            for name in self.COLUMNS
        )
        with self.postgres_connection() as connection, connection.cursor() as cursor:
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS "premisesFurnitureEquipment" (
                    id BIGSERIAL PRIMARY KEY,
                    "recordKey" VARCHAR(64) UNIQUE,
                    {column_defs}
                )
            ''')
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_premisesFurnitureEquipment_record_key ON "premisesFurnitureEquipment" ("recordKey")')
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
            self._text(value) if name in self.TEXT_COLUMNS else self._number(value)
            for name, value in zip(self.COLUMNS, row[:len(self.COLUMNS)])
        ]
        # Record identity uses ASSET_MASTER's own primary key (ASSET_ID),
        # carried through as sourceAssetId at the end of the SELECT list.
        key_value = row[len(self.COLUMNS)]
        record_key = hashlib.sha256(('' if key_value is None else str(key_value)).encode('utf-8')).hexdigest()
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
            self.logger.exception('Premises-furniture-equipment producer failed')
        finally:
            self.producer_finished.set()
            if connection:
                connection.close()

    def insert_batch(self, records):
        unique = {record['recordKey']: record for record in records}
        values = [(record['recordKey'], *record['values']) for record in unique.values()]
        columns_sql = ', '.join(f'"{name}"' for name in self.COLUMNS)
        update_sql = ', '.join(f'"{name}"=EXCLUDED."{name}"' for name in self.COLUMNS)
        with self.postgres_connection() as connection, connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, f'''
                INSERT INTO "premisesFurnitureEquipment" ("recordKey", {columns_sql}) VALUES %s
                ON CONFLICT ("recordKey") DO UPDATE SET {update_sql}
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
            self.logger.exception('Premises-furniture-equipment consumer failed')
        finally:
            connection.close()

    def run_streaming_pipeline(self):
        self.setup_target_table()
        self.setup_queue()
        consumer = threading.Thread(target=self.consumer_thread, name='premises-furniture-equipment-consumer')
        producer = threading.Thread(target=self.producer_thread, name='premises-furniture-equipment-producer')
        consumer.start(); producer.start(); producer.join(); consumer.join(timeout=120)
        if consumer.is_alive():
            self.stop_consumer.set(); consumer.join(timeout=30)
        if consumer.is_alive():
            raise RuntimeError('Premises-furniture-equipment consumer did not stop')
        if self._thread_error:
            raise RuntimeError('Premises-furniture-equipment pipeline failed') from self._thread_error
        self.logger.info('Premises-furniture-equipment pipeline completed: produced=%s, consumed=%s', self.total_produced, self.total_consumed)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    PremisesFurnitureEquipmentStreamingPipeline().run_streaming_pipeline()
