#!/usr/bin/env python3
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cash_statistics.cash_statistics_streaming_pipeline import CashStatisticsStreamingPipeline

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    CashStatisticsStreamingPipeline().run_streaming_pipeline()
