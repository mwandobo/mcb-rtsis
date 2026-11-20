#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the insurance-commission pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from insurance_commission.insurance_commission_streaming_pipeline import InsuranceCommissionStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    InsuranceCommissionStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Insurance-commission target table is ready.')
