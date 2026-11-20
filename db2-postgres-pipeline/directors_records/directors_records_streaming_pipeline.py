#!/usr/bin/env python3
"""DB2 → RabbitMQ → PostgreSQL pipeline for directors-records.sql."""
import hashlib, json, logging, os, sys, threading, time
from contextlib import contextmanager
from typing import Optional
import pika, psycopg2, psycopg2.extras

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from db2_connection import DB2Connection


class DirectorsRecordsStreamingPipeline:
    target_table = '"directorsRecords"'
    queue_name, dead_letter_queue, dead_letter_exchange = 'directors_records_queue', 'directors_records_dead_letter', 'directors_records_dlx'

    def __init__(self, batch_size=1000, consumer_batch_size=100):
        self.config, self.db2_conn = Config(), DB2Connection()
        self.batch_size, self.consumer_batch_size = batch_size, consumer_batch_size
        self.producer_finished, self.stop_consumer = threading.Event(), threading.Event()
        self.total_produced = self.total_consumed = 0
        self._stats_lock, self._thread_error = threading.Lock(), None
        self.logger = logging.getLogger(__name__)

    def get_query(self):
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(root, 'sqls', 'directors-records.sql'), encoding='utf-8') as file: return file.read().strip()

    @contextmanager
    def postgres_connection(self):
        c = self.config.database
        conn = psycopg2.connect(host=c.pg_host, port=c.pg_port, dbname=c.pg_database, user=c.pg_user, password=c.pg_password)
        try: yield conn
        finally: conn.close()

    def setup_target_table(self):
        with self.postgres_connection() as conn, conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS "directorsRecords" (
                id BIGSERIAL PRIMARY KEY, "recordKey" VARCHAR(64) NOT NULL UNIQUE,
                "reportingDate" VARCHAR(12), "branchCode" VARCHAR(50), "directorName" VARCHAR(255),
                "directorDob" VARCHAR(12), "directorNin" VARCHAR(100), "directorCategory" VARCHAR(100),
                "directorClass" VARCHAR(100), "appointmentDate" VARCHAR(12), gender VARCHAR(20),
                "committeeMembership" VARCHAR(255), "directorFees" NUMERIC(24,2),
                "directorBenefits" JSONB, "directorAllowance" NUMERIC(24,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_directorsRecords_nin ON "directorsRecords"("directorNin")')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_directorsRecords_branch ON "directorsRecords"("branchCode")')
            conn.commit()

    def rabbitmq_connection(self):
        q = self.config.message_queue
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=q.rabbitmq_host, port=q.rabbitmq_port, credentials=pika.PlainCredentials(q.rabbitmq_user, q.rabbitmq_password), heartbeat=600, blocked_connection_timeout=300))
        return conn, conn.channel()

    def setup_queue(self):
        conn, ch = self.rabbitmq_connection()
        try:
            ch.exchange_declare(exchange=self.dead_letter_exchange, exchange_type='direct', durable=True)
            ch.queue_declare(queue=self.dead_letter_queue, durable=True)
            ch.queue_bind(queue=self.dead_letter_queue, exchange=self.dead_letter_exchange, routing_key=self.queue_name)
            ch.queue_declare(queue=self.queue_name, durable=True, arguments={'x-dead-letter-exchange': self.dead_letter_exchange, 'x-dead-letter-routing-key': self.queue_name})
        finally: conn.close()

    @staticmethod
    def text(v): return None if v is None else str(v).strip()
    @staticmethod
    def number(v): return None if v is None else float(v)

    def process_record(self, row):
        values = [self.text(row[i]) if i not in (10, 12) else self.number(row[i]) for i in range(13)]
        try: json.loads(values[11] or '[]')
        except json.JSONDecodeError: values[11] = '[]'
        key = hashlib.sha256('|'.join(str(values[i] or '') for i in (1,2,4,7)).encode()).hexdigest()
        return {'recordKey': key, 'values': values}

    def producer_thread(self):
        conn = None
        try:
            conn, ch = self.rabbitmq_connection()
            with self.db2_conn.get_connection(log_connection=True) as db2:
                cur = db2.cursor(); cur.execute(self.get_query())
                while rows := cur.fetchmany(self.batch_size):
                    for row in rows:
                        ch.basic_publish(exchange='', routing_key=self.queue_name, body=json.dumps(self.process_record(row)), properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))
                        with self._stats_lock: self.total_produced += 1
        except BaseException as error:
            self._thread_error = error; self.logger.exception('Directors producer failed')
        finally:
            self.producer_finished.set()
            if conn: conn.close()

    def insert_batch(self, records):
        unique = {r['recordKey']: r for r in records}
        rows = [(r['recordKey'], r['values'][0], r['values'][1], r['values'][2], r['values'][3], r['values'][4], r['values'][5], r['values'][6], r['values'][7], r['values'][8], r['values'][9], r['values'][10], psycopg2.extras.Json(json.loads(r['values'][11] or '[]')), r['values'][12]) for r in unique.values()]
        with self.postgres_connection() as conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, '''INSERT INTO "directorsRecords" (
                "recordKey", "reportingDate", "branchCode", "directorName", "directorDob", "directorNin", "directorCategory", "directorClass", "appointmentDate", gender, "committeeMembership", "directorFees", "directorBenefits", "directorAllowance") VALUES %s
                ON CONFLICT ("recordKey") DO UPDATE SET "reportingDate"=EXCLUDED."reportingDate", "directorBenefits"=EXCLUDED."directorBenefits", "directorFees"=EXCLUDED."directorFees", "directorAllowance"=EXCLUDED."directorAllowance", updated_at=CURRENT_TIMESTAMP''', rows)
            conn.commit()

    def consumer_thread(self):
        conn, ch = self.rabbitmq_connection(); records=[]; tags=[]; idle=0
        try:
            while not self.stop_consumer.is_set():
                method, _, body = ch.basic_get(queue=self.queue_name, auto_ack=False)
                if method is None:
                    idle = idle + 1 if self.producer_finished.is_set() else 0
                    if idle >= 3: break
                    time.sleep(1); continue
                idle=0; records.append(json.loads(body.decode())); tags.append(method.delivery_tag)
                if len(records) >= self.consumer_batch_size:
                    self.insert_batch(records)
                    for tag in tags: ch.basic_ack(tag)
                    with self._stats_lock: self.total_consumed += len(records)
                    records=[]; tags=[]
            if records:
                self.insert_batch(records)
                for tag in tags: ch.basic_ack(tag)
                with self._stats_lock: self.total_consumed += len(records)
        except BaseException as error:
            self._thread_error = error; self.logger.exception('Directors consumer failed')
        finally: conn.close()

    def run_streaming_pipeline(self):
        # No TMSTAMP (or any last-modified column) exists on BOARD_OF_DIRECTORS,
        # only APPOINTMENT_DATE (a business event date, not a row-modified
        # marker) - filtering on it would silently miss fee/benefit updates on
        # already-appointed directors. The table is tiny (9 rows), so this
        # deliberately always does a full scan; insert_batch's upsert
        # (ON CONFLICT ... DO UPDATE) is what keeps re-runs correct and
        # picks up changed fees/benefits/allowance, not an incremental filter.
        self._update_state('running')
        try:
            self.setup_target_table(); self.setup_queue()
            consumer=threading.Thread(target=self.consumer_thread); producer=threading.Thread(target=self.producer_thread)
            consumer.start(); producer.start(); producer.join(); consumer.join(timeout=90)
            if consumer.is_alive(): self.stop_consumer.set(); consumer.join(timeout=30)
            if consumer.is_alive(): raise RuntimeError('Directors consumer did not stop')
            if self._thread_error: raise RuntimeError('Directors pipeline failed') from self._thread_error
            self.logger.info('Directors pipeline completed: produced=%s consumed=%s', self.total_produced, self.total_consumed)
            self._update_state('completed')
        except Exception as e:
            self._update_state('failed', str(e))
            raise

    def _update_state(self, status, error_message=None):
        try:
            from pipeline_state import PipelineStateManager
            PipelineStateManager().update_run('directors_records', status, self.total_consumed, error_message)
            self.logger.info(f"State updated: {status}, records: {self.total_consumed}")
        except Exception as e:
            self.logger.warning(f"Could not update state: {e}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    DirectorsRecordsStreamingPipeline().run_streaming_pipeline()
