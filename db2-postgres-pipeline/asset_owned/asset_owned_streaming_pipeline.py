#!/usr/bin/env python3
"""Stream the DB2 asset-owned report into PostgreSQL through RabbitMQ."""

import hashlib
import json
import logging
import os
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import List, Optional, Tuple

import pika
import psycopg2
import psycopg2.extras

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection


@dataclass
class AssetOwnedRecord:
    reportingDate: Optional[str]
    acquisitionDate: Optional[str]
    currency: Optional[str]
    assetCategory: Optional[str]
    assetType: Optional[str]
    orgCostValue: Optional[float]
    usdCostValue: Optional[float]
    tzsCostValue: Optional[float]
    allowanceProbableLoss: Optional[float]
    botProvision: Optional[float]
    recordKey: str


class AssetOwnedStreamingPipeline:
    """Producer/consumer pipeline for ``sqls/asset_owned.sql``."""

    target_table = '"assetOwned"'
    queue_name = "asset_owned_queue"
    dead_letter_queue = "asset_owned_dead_letter"
    dead_letter_exchange = "asset_owned_dlx"

    def __init__(self, batch_size: int = 1000, consumer_batch_size: int = 100):
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

    def get_query(self) -> str:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(project_root, "sqls", "asset_owned.sql"), encoding="utf-8") as sql_file:
            return sql_file.read().strip()

    @contextmanager
    def postgres_connection(self):
        conn = psycopg2.connect(
            host=self.config.database.pg_host,
            port=self.config.database.pg_port,
            database=self.config.database.pg_database,
            user=self.config.database.pg_user,
            password=self.config.database.pg_password,
        )
        try:
            yield conn
        finally:
            conn.close()

    def setup_target_table(self) -> None:
        """Create the target once, without deleting an existing load."""
        with self.postgres_connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS "assetOwned" (
                    id BIGSERIAL PRIMARY KEY,
                    "recordKey" VARCHAR(64) NOT NULL UNIQUE,
                    "reportingDate" VARCHAR(50),
                    "acquisitionDate" VARCHAR(50),
                    currency VARCHAR(20),
                    "assetCategory" VARCHAR(50),
                    "assetType" VARCHAR(100),
                    "orgCostValue" NUMERIC(24, 2),
                    "usdCostValue" NUMERIC(24, 2),
                    "tzsCostValue" NUMERIC(24, 2),
                    "allowanceProbableLoss" NUMERIC(24, 2),
                    "botProvision" NUMERIC(24, 2),
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )
            cursor.execute('ALTER TABLE "assetOwned" ADD COLUMN IF NOT EXISTS "recordKey" VARCHAR(64)')
            cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_assetOwned_record_key ON "assetOwned" ("recordKey")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_assetOwned_acquisition_date ON "assetOwned" ("acquisitionDate")')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_assetOwned_category ON "assetOwned" ("assetCategory")')
            conn.commit()

    def rabbitmq_connection(self) -> Tuple[pika.BlockingConnection, object]:
        credentials = pika.PlainCredentials(
            self.config.message_queue.rabbitmq_user,
            self.config.message_queue.rabbitmq_password,
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )
        )
        return connection, connection.channel()

    def setup_queue(self) -> None:
        connection, channel = self.rabbitmq_connection()
        try:
            channel.exchange_declare(exchange=self.dead_letter_exchange, exchange_type="direct", durable=True)
            channel.queue_declare(queue=self.dead_letter_queue, durable=True)
            channel.queue_bind(queue=self.dead_letter_queue, exchange=self.dead_letter_exchange, routing_key=self.queue_name)
            channel.queue_declare(
                queue=self.queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": self.dead_letter_exchange,
                    "x-dead-letter-routing-key": self.queue_name,
                },
            )
        finally:
            connection.close()

    @staticmethod
    def _text(value) -> Optional[str]:
        return None if value is None else str(value).strip()

    @staticmethod
    def _number(value) -> Optional[float]:
        return None if value is None else float(value)

    @staticmethod
    def _key_amount(value) -> str:
        """Use a fixed decimal representation in a stable asset identity."""
        return "" if value is None else f"{float(value):.2f}"

    @staticmethod
    def _postgres_timestamp(value: Optional[str]):
        """Convert DB2 VARCHAR(CURRENT_TIMESTAMP) into a PostgreSQL timestamp."""
        if value is None:
            return None
        for format_string in (
            "%Y-%m-%d-%H.%M.%S.%f",
            "%Y-%m-%d-%H.%M.%S",
            "%d%m%Y%H%M",
        ):
            try:
                return datetime.strptime(value, format_string)
            except ValueError:
                continue
        raise ValueError(f"Unsupported DB2 reporting timestamp: {value!r}")

    @staticmethod
    def _postgres_date(value: Optional[str]) -> Optional[date]:
        if value is None:
            return None
        for format_string in ("%Y-%m-%d", "%d%m%Y%H%M"):
            try:
                return datetime.strptime(value, format_string).date()
            except ValueError:
                continue
        raise ValueError(f"Unsupported DB2 acquisition date: {value!r}")

    def process_record(self, row) -> AssetOwnedRecord:
        values = [self._text(value) for value in row[1:10]]
        # Exclude FX-derived amounts: changes to a fixing rate must update the
        # same asset, not create a second record.
        acquisition_date = self._postgres_date(values[0])
        key_values = [
            acquisition_date.isoformat() if acquisition_date else "",
            values[1] or "",
            values[2] or "",
            values[3] or "",
            self._key_amount(row[5]),
        ]
        record_key = hashlib.sha256("|".join(key_values).encode("utf-8")).hexdigest()
        return AssetOwnedRecord(
            reportingDate=self._text(row[0]),
            acquisitionDate=values[0],
            currency=values[1],
            assetCategory=values[2],
            assetType=values[3],
            orgCostValue=self._number(row[5]),
            usdCostValue=self._number(row[6]),
            tzsCostValue=self._number(row[7]),
            allowanceProbableLoss=self._number(row[8]),
            botProvision=self._number(row[9]),
            recordKey=record_key,
        )

    def producer_thread(self) -> None:
        connection = None
        try:
            connection, channel = self.rabbitmq_connection()
            with self.db2_conn.get_connection(log_connection=True) as db2_conn:
                cursor = db2_conn.cursor()
                cursor.execute(self.get_query())
                while True:
                    rows = cursor.fetchmany(self.batch_size)
                    if not rows:
                        break
                    for row in rows:
                        record = self.process_record(row)
                        channel.basic_publish(
                            exchange="",
                            routing_key=self.queue_name,
                            body=json.dumps(asdict(record)),
                            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
                        )
                        with self._stats_lock:
                            self.total_produced += 1
            self.logger.info("Asset-owned producer finished: %s records", self.total_produced)
        except BaseException as error:
            self._thread_error = error
            self.logger.exception("Asset-owned producer failed")
        finally:
            self.producer_finished.set()
            if connection:
                connection.close()

    def insert_batch(self, records: List[AssetOwnedRecord]) -> None:
        # One source extract can contain repeated rows.  PostgreSQL cannot apply
        # ON CONFLICT DO UPDATE twice to the same key within one INSERT, so retain
        # the last copy of each record before sending the batch.
        unique_records = {record.recordKey: record for record in records}.values()
        values = [
            (
                # BoT requires the literal DDMMYYYYHHMM representation emitted
                # by asset_owned.sql, so retain those source strings in the target.
                record.recordKey, record.reportingDate, record.acquisitionDate, record.currency,
                record.assetCategory, record.assetType, record.orgCostValue, record.usdCostValue,
                record.tzsCostValue, record.allowanceProbableLoss, record.botProvision,
            )
            for record in unique_records
        ]
        with self.postgres_connection() as conn, conn.cursor() as cursor:
            psycopg2.extras.execute_values(
                cursor,
                '''
                INSERT INTO "assetOwned" (
                    "recordKey", "reportingDate", "acquisitionDate", currency, "assetCategory", "assetType",
                    "orgCostValue", "usdCostValue", "tzsCostValue", "allowanceProbableLoss", "botProvision"
                ) VALUES %s
                ON CONFLICT ("recordKey") DO UPDATE SET
                    "reportingDate" = EXCLUDED."reportingDate",
                    "acquisitionDate" = EXCLUDED."acquisitionDate",
                    currency = EXCLUDED.currency,
                    "assetCategory" = EXCLUDED."assetCategory",
                    "assetType" = EXCLUDED."assetType",
                    "orgCostValue" = EXCLUDED."orgCostValue",
                    "usdCostValue" = EXCLUDED."usdCostValue",
                    "tzsCostValue" = EXCLUDED."tzsCostValue",
                    "allowanceProbableLoss" = EXCLUDED."allowanceProbableLoss",
                    "botProvision" = EXCLUDED."botProvision"
                ''',
                values,
            )
            conn.commit()

    def consumer_thread(self) -> None:
        connection, channel = self.rabbitmq_connection()
        records: List[AssetOwnedRecord] = []
        delivery_tags: List[int] = []
        idle_polls = 0
        try:
            while not self.stop_consumer.is_set():
                method, _, body = channel.basic_get(queue=self.queue_name, auto_ack=False)
                if method is None:
                    if self.producer_finished.is_set():
                        idle_polls += 1
                        if idle_polls >= 3:
                            break
                    time.sleep(1)
                    continue
                idle_polls = 0
                records.append(AssetOwnedRecord(**json.loads(body.decode("utf-8"))))
                delivery_tags.append(method.delivery_tag)
                if len(records) >= self.consumer_batch_size:
                    self.insert_batch(records)
                    for tag in delivery_tags:
                        channel.basic_ack(tag)
                    with self._stats_lock:
                        self.total_consumed += len(records)
                    records, delivery_tags = [], []
            if records:
                self.insert_batch(records)
                for tag in delivery_tags:
                    channel.basic_ack(tag)
                with self._stats_lock:
                    self.total_consumed += len(records)
        except BaseException as error:
            self._thread_error = error
            self.logger.exception("Asset-owned consumer failed")
        finally:
            connection.close()

    def run_streaming_pipeline(self) -> None:
        self.setup_target_table()
        self.setup_queue()
        consumer = threading.Thread(target=self.consumer_thread, name="asset-owned-consumer")
        producer = threading.Thread(target=self.producer_thread, name="asset-owned-producer")
        consumer.start()
        producer.start()
        producer.join()
        consumer.join(timeout=90)
        if consumer.is_alive():
            self.stop_consumer.set()
            consumer.join(timeout=30)
        if consumer.is_alive():
            raise RuntimeError("Asset-owned consumer did not stop")
        if self._thread_error:
            raise RuntimeError("Asset-owned pipeline failed") from self._thread_error
        self.logger.info("Asset-owned pipeline completed: produced=%s, consumed=%s", self.total_produced, self.total_consumed)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    AssetOwnedStreamingPipeline().run_streaming_pipeline()
