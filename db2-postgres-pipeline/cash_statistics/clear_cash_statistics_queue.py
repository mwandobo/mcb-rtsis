#!/usr/bin/env python3
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cash_statistics.cash_statistics_streaming_pipeline import CashStatisticsStreamingPipeline

if __name__ == '__main__':
    pipeline = CashStatisticsStreamingPipeline()
    pipeline.setup_queue()
    conn, channel = pipeline.rabbitmq_connection()
    try:
        for queue in (pipeline.queue_name, pipeline.dead_letter_queue):
            channel.queue_purge(queue=queue)
    finally:
        conn.close()
