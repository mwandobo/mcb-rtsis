#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the digital-credit pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from digital_credit.digital_credit_streaming_pipeline import DigitalCreditStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    DigitalCreditStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Digital-credit target table is ready.')
