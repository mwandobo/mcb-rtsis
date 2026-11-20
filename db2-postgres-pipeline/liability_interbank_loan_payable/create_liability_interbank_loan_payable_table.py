#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the liability-interbank-loan-payable pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from liability_interbank_loan_payable.liability_interbank_loan_payable_streaming_pipeline import LiabilityInterbankLoanPayableStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    LiabilityInterbankLoanPayableStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Liability-interbank-loan-payable target table is ready.')
