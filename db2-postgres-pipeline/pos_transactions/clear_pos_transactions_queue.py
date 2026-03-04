#!/usr/bin/env python3
"""
Clear the pos_transactions_queue and pos_transactions_dead_letter queues in RabbitMQ.
"""

import sys
import os
import pika
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config

def clear_queue(queue_name):
    config = Config()
    credentials = pika.PlainCredentials(
        config.message_queue.rabbitmq_user,
        config.message_queue.rabbitmq_password
    )
    parameters = pika.ConnectionParameters(
        host=config.message_queue.rabbitmq_host,
        port=config.message_queue.rabbitmq_port,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_purge(queue=queue_name)
    print(f"Queue '{queue_name}' cleared.")
    connection.close()

def main():
    clear_queue('pos_transactions_queue')
    clear_queue('pos_transactions_dead_letter')

if __name__ == "__main__":
    main()
