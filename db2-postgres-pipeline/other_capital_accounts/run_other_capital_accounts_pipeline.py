#!/usr/bin/env python3
"""Run the other-capital-accounts streaming pipeline."""
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from other_capital_accounts.other_capital_accounts_streaming_pipeline import OtherCapitalAccountsStreamingPipeline

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    OtherCapitalAccountsStreamingPipeline(batch_size=1000, consumer_batch_size=100).run_streaming_pipeline()
