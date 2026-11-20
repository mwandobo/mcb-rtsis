#!/usr/bin/env python3
"""Run the unearned-income streaming pipeline."""
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from unearned_income.unearned_income_streaming_pipeline import UnearnedIncomeStreamingPipeline

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    UnearnedIncomeStreamingPipeline(batch_size=1000, consumer_batch_size=100).run_streaming_pipeline()
