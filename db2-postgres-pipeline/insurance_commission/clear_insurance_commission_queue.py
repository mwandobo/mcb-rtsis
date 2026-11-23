#!/usr/bin/env python3
"""Purge the insurance-commission RabbitMQ work and dead-letter queues."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from insurance_commission._deprecated_db2_based_insurance_commission_streaming_pipeline import InsuranceCommissionStreamingPipeline


def clear_queues():
    pipeline = InsuranceCommissionStreamingPipeline()
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
