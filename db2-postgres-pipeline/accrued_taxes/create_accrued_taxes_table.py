#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the accrued-taxes pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from accrued_taxes.accrued_taxes_streaming_pipeline import AccruedTaxesStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    AccruedTaxesStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Accrued-taxes target table is ready.')
