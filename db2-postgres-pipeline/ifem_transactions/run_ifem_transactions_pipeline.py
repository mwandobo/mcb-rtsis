#!/usr/bin/env python3
"""Run the IFEM-transactions streaming pipeline."""
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ifem_transactions.ifem_transactions_streaming_pipeline import IfemTransactionsStreamingPipeline

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    IfemTransactionsStreamingPipeline(batch_size=1000, consumer_batch_size=100).run_streaming_pipeline()
