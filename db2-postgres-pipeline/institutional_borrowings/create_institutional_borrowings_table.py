#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the institutional-borrowings pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from institutional_borrowings.institutional_borrowings_streaming_pipeline import InstitutionalBorrowingsStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    InstitutionalBorrowingsStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Institutional-borrowings target table is ready.')
