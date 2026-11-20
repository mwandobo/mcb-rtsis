#!/usr/bin/env python3
"""Run the liability-interbank-loan-payable streaming pipeline."""
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from liability_interbank_loan_payable.liability_interbank_loan_payable_streaming_pipeline import LiabilityInterbankLoanPayableStreamingPipeline

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    LiabilityInterbankLoanPayableStreamingPipeline(batch_size=1000, consumer_batch_size=100).run_streaming_pipeline()
