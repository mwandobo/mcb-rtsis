#!/usr/bin/env python3
"""
Consumer script to process remaining messages from balances_with_mnos_queue
"""

import pika
import psycopg2
import json
import logging
import time
from decimal import Decimal
from contextlib import contextmanager

from config import Config


class BalancesWithMnosConsumer:
    def __init__(self):
        self.config = Config()
        self.total_consumed = 0
        self.start_time = time.time()
        self.max_retries = 3
        self.retry_delay = 2

        # Setup logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        self.logger.info("Balances with MNOs Consumer initialized")

    @contextmanager
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password,
            )
            yield conn
        except Exception as e:
            self.logger.error(f"PostgreSQL connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def setup_rabbitmq_connection(self, max_retries=3):
        """Setup RabbitMQ connection with retry logic"""
        for attempt in range(max_retries):
            try:
                credentials = pika.PlainCredentials(
                    self.config.message_queue.rabbitmq_user,
                    self.config.message_queue.rabbitmq_password,
                )
                parameters = pika.ConnectionParameters(
                    host=self.config.message_queue.rabbitmq_host,
                    port=self.config.message_queue.rabbitmq_port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                return connection, channel

            except Exception as e:
                self.logger.warning(
                    f"RabbitMQ connection attempt {attempt + 1} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

    def insert_to_postgres(self, record_data, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "balancesWithMnos" (
            "reportingDate", "floatBalanceDate", "mnoCode", "tillNumber",
            "currency", "allowanceProbableLoss", "botProvision",
            "orgFloatAmount", "usdFloatAmount", "tzsFloatAmount"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            insert_sql,
            (
                record_data.get('reportingDate'),
                record_data.get('floatBalanceDate'),
                record_data.get('mnoCode'),
                record_data.get('tillNumber'),
                record_data.get('currency'),
                record_data.get('allowanceProbableLoss'),
                record_data.get('botProvision'),
                record_data.get('orgFloatAmount'),
                record_data.get('usdFloatAmount'),
                record_data.get('tzsFloatAmount'),
            ),
        )

    def get_queue_message_count(self):
        """Get the number of messages in the queue"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            method = channel.queue_declare(
                queue="balances_with_mnos_queue", durable=True, passive=True
            )
            message_count = method.method.message_count
            connection.close()
            return message_count
        except Exception as e:
            self.logger.error(f"Failed to get queue message count: {e}")
            return 0

    def consume_messages(self):
        """Consume all messages from the queue"""
        try:
            # Check queue size
            initial_count = self.get_queue_message_count()
            self.logger.info(f"Queue has {initial_count:,} messages to process")

            if initial_count == 0:
                self.logger.info("No messages in queue. Exiting.")
                return

            # Setup RabbitMQ connection
            connection, channel = self.setup_rabbitmq_connection()
            last_progress_report = time.time()

            def process_message(ch, method, properties, body):
                nonlocal last_progress_report
                try:
                    record_data = json.loads(body)

                    # Convert string decimals back to Decimal
                    if record_data.get('allowanceProbableLoss'):
                        record_data['allowanceProbableLoss'] = Decimal(
                            record_data['allowanceProbableLoss']
                        )
                    if record_data.get('botProvision'):
                        record_data['botProvision'] = Decimal(record_data['botProvision'])
                    if record_data.get('orgFloatAmount'):
                        record_data['orgFloatAmount'] = Decimal(
                            record_data['orgFloatAmount']
                        )
                    if record_data.get('usdFloatAmount'):
                        record_data['usdFloatAmount'] = Decimal(
                            record_data['usdFloatAmount']
                        )
                    if record_data.get('tzsFloatAmount'):
                        record_data['tzsFloatAmount'] = Decimal(
                            record_data['tzsFloatAmount']
                        )

                    # Insert to PostgreSQL with retry
                    inserted = False
                    for attempt in range(self.max_retries):
                        try:
                            with self.get_postgres_connection() as conn:
                                cursor = conn.cursor()
                                self.insert_to_postgres(record_data, cursor)
                                conn.commit()
                            inserted = True
                            break
                        except Exception as e:
                            self.logger.warning(
                                f"PostgreSQL insert attempt {attempt + 1} failed: {e}"
                            )
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                            else:
                                self.logger.error(
                                    f"Failed to insert record after {self.max_retries} attempts"
                                )

                    if inserted:
                        self.total_consumed += 1

                        # Log progress every 100 records
                        if self.total_consumed % 100 == 0:
                            elapsed_time = time.time() - self.start_time
                            rate = (
                                self.total_consumed / elapsed_time
                                if elapsed_time > 0
                                else 0
                            )
                            remaining = initial_count - self.total_consumed
                            self.logger.info(
                                f"Processed {self.total_consumed:,}/{initial_count:,} records "
                                f"({remaining:,} remaining) - Rate: {rate:.1f} rec/sec"
                            )

                        # Detailed progress report every 5 minutes
                        current_time = time.time()
                        if current_time - last_progress_report >= 300:
                            elapsed_time = current_time - self.start_time
                            rate = (
                                self.total_consumed / elapsed_time
                                if elapsed_time > 0
                                else 0
                            )
                            remaining = initial_count - self.total_consumed
                            progress_pct = (
                                (self.total_consumed / initial_count * 100)
                                if initial_count > 0
                                else 0
                            )

                            self.logger.info(
                                f"PROGRESS REPORT: {self.total_consumed:,}/{initial_count:,} records "
                                f"({progress_pct:.1f}%) - Rate: {rate:.1f} rec/sec - "
                                f"Time: {elapsed_time/60:.1f} min"
                            )
                            last_progress_report = current_time

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            # Start consuming
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(
                queue="balances_with_mnos_queue", on_message_callback=process_message
            )

            self.logger.info("Starting to consume messages...")

            # Process messages until queue is empty
            while True:
                try:
                    connection.process_data_events(time_limit=1)

                    # Check if queue is empty
                    method = channel.queue_declare(
                        queue="balances_with_mnos_queue", durable=True, passive=True
                    )
                    if method.method.message_count == 0:
                        self.logger.info("Queue is empty. All messages processed.")
                        break

                except Exception as e:
                    self.logger.error(f"Error during message processing: {e}")
                    try:
                        connection.close()
                    except:
                        pass
                    # Reconnect
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=10)
                    channel.basic_consume(
                        queue="balances_with_mnos_queue",
                        on_message_callback=process_message,
                    )

            connection.close()

            # Final statistics
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0

            self.logger.info(
                f"""
            ==========================================
            Consumer Summary:
            ==========================================
            Initial queue size: {initial_count:,}
            Records consumed: {self.total_consumed:,}
            Total processing time: {total_time/60:.2f} minutes
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """
            )

        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            raise


def main():
    """Main function"""
    print("=" * 60)
    print("Balances with MNOs - Remaining Messages Consumer")
    print("=" * 60)

    consumer = BalancesWithMnosConsumer()

    try:
        consumer.consume_messages()
        print("\n" + "=" * 60)
        print("Consumer completed successfully!")
        print("=" * 60)
    except KeyboardInterrupt:
        print("\n\nConsumer stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"\n\nConsumer failed with error: {e}")
        import sys

        sys.exit(1)


if __name__ == "__main__":
    main()
