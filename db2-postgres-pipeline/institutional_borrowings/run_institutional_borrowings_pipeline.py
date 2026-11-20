#!/usr/bin/env python3
"""Run the institutional-borrowings streaming pipeline."""
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from institutional_borrowings.institutional_borrowings_streaming_pipeline import InstitutionalBorrowingsStreamingPipeline

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    InstitutionalBorrowingsStreamingPipeline(batch_size=1000, consumer_batch_size=100).run_streaming_pipeline()
