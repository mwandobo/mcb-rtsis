#!/usr/bin/env python3
"""
Consume remaining messages from cheques queue and process them
"""

import pika
import psycopg2
import psycopg2.extras
import json
import sys
import os
from dataclasses import dataclass
from typing import List

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

@dataclass
class ChequeRecord:
    """Data class for cheque records"""
    reportingDate: str
    chequeNumber: str
    issuerName: str
    issuerBankerCode: str
    payeeName: str
    payeeAccountNumber: str
    chequeDate: str
    transactionDate: str
    settlementDate: str
    allowanceProbableLoss: str
    botProvision: str
    currency: str
    orgAmountOpening: str
    usdAmountOpening: str
    tzsAmountOpening: str
    orgAmountPayment: str
    usdAmountPayment: str
    tzsAmountPayment: str
    orgAmountBalance: str
    usdAmountBalance: str
    tzsAmountBalance: str
    tmstamp: str

def consume_remaining_messages():
    """Consume remaining messages from cheques queue"""
    config = Config()
    
    try:
        # Setup RabbitMQ connection
        credentials = pika.PlainCredentials(
            config.message_queue.rabbitmq_user,
            config.message_queue.rabbitmq_password
        )
        parameters = pika.ConnectionParameters(
            host=config.message_queue.rabbitmq_host,
            port=config.message_queue.rabbitmq_port,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Check queue status
        queue_state = channel.queue_declare(queue='cheques_queue', durable=True, passive=True)
        message_count = queue_state.method.message_count
        print(f"📊 Found {message_count:,} messages in cheques_queue")
        
        if message_count == 0:
            print("✅ No messages to consume")
            connection.close()
            return
        
        # Setup PostgreSQL connection
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        print("✅ PostgreSQL connection established")
        
        # Create table if it doesn't exist
        cursor = pg_conn.cursor()
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS "chequeClearing" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(50),
            "chequeNumber" VARCHAR(100),
            "issuerName" VARCHAR(255),
            "issuerBankerCode" VARCHAR(50),
            "payeeName" VARCHAR(255),
            "payeeAccountNumber" VARCHAR(100),
            "chequeDate" VARCHAR(50),
            "transactionDate" VARCHAR(50),
            "settlementDate" VARCHAR(50),
            "allowanceProbableLoss" VARCHAR(50),
            "botProvision" VARCHAR(50),
            "currency" VARCHAR(10),
            "orgAmountOpening" VARCHAR(50),
            "usdAmountOpening" VARCHAR(50),
            "tzsAmountOpening" VARCHAR(50),
            "orgAmountPayment" VARCHAR(50),
            "usdAmountPayment" VARCHAR(50),
            "tzsAmountPayment" VARCHAR(50),
            "orgAmountBalance" VARCHAR(50),
            "usdAmountBalance" VARCHAR(50),
            "tzsAmountBalance" VARCHAR(50),
            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)
        pg_conn.commit()
        print("✅ Table ensured to exist")
        
        # Batch processing variables
        batch_size = 100
        insert_buffer: List[ChequeRecord] = []
        processed_count = 0
        
        def flush_buffer():
            """Flush buffered records to PostgreSQL"""
            nonlocal insert_buffer, processed_count
            if not insert_buffer:
                return
            
            try:
                insert_query = """
                INSERT INTO "chequeClearing" (
                    "reportingDate", "chequeNumber", "issuerName", "issuerBankerCode", "payeeName",
                    "payeeAccountNumber", "chequeDate", "transactionDate", "settlementDate",
                    "allowanceProbableLoss", "botProvision", "currency", "orgAmountOpening",
                    "usdAmountOpening", "tzsAmountOpening", "orgAmountPayment", "usdAmountPayment",
                    "tzsAmountPayment", "orgAmountBalance", "usdAmountBalance", "tzsAmountBalance"
                ) VALUES %s
                """
                
                values = [
                    (
                        r.reportingDate, r.chequeNumber, r.issuerName, r.issuerBankerCode, r.payeeName,
                        r.payeeAccountNumber, r.chequeDate, r.transactionDate, r.settlementDate,
                        r.allowanceProbableLoss, r.botProvision, r.currency, r.orgAmountOpening,
                        r.usdAmountOpening, r.tzsAmountOpening, r.orgAmountPayment, r.usdAmountPayment,
                        r.tzsAmountPayment, r.orgAmountBalance, r.usdAmountBalance, r.tzsAmountBalance
                    )
                    for r in insert_buffer
                ]
                
                psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
                pg_conn.commit()
                
                processed_count += len(insert_buffer)
                print(f"✅ Processed batch: {len(insert_buffer)} records (Total: {processed_count})")
                insert_buffer = []
                
            except Exception as e:
                print(f"❌ Error inserting batch: {e}")
                pg_conn.rollback()
                insert_buffer = []
        
        # Process messages one by one
        while True:
            method_frame, header_frame, body = channel.basic_get(queue='cheques_queue', auto_ack=False)
            
            if method_frame is None:
                # No more messages
                break
            
            try:
                # Parse message
                record_data = json.loads(body)
                record = ChequeRecord(**record_data)
                
                # Add to buffer
                insert_buffer.append(record)
                
                # Flush if buffer is full
                if len(insert_buffer) >= batch_size:
                    flush_buffer()
                
                # Acknowledge message
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                # Reject message (send to dead letter queue)
                channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)
        
        # Flush remaining buffer
        flush_buffer()
        
        # Close connections
        connection.close()
        pg_conn.close()
        
        print(f"🎉 Successfully consumed and processed {processed_count} messages from the queue")
        
    except Exception as e:
        print(f"❌ Error consuming messages: {e}")
        sys.exit(1)

if __name__ == "__main__":
    consume_remaining_messages()