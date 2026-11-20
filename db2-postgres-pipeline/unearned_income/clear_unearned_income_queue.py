#!/usr/bin/env python3
"""Purge the unearned-income RabbitMQ work and dead-letter queues."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unearned_income.unearned_income_streaming_pipeline import UnearnedIncomeStreamingPipeline


def clear_queues():
    pipeline = UnearnedIncomeStreamingPipeline()
    pipeline.setup_queue()
    connection, channel = pipeline.rabbitmq_connection()
    try:
        for queue_name in (pipeline.queue_name, pipeline.dead_letter_queue):
            result = channel.queue_purge(queue=queue_name)
            logging.getLogger(__name__).info('Purged %s message(s) from %s.', result.method.message_count, queue_name)
    finally:
        connection.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    clear_queues()
