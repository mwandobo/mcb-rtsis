#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the IFEM-transactions pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ifem_transactions.ifem_transactions_streaming_pipeline import IfemTransactionsStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    IfemTransactionsStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('IFEM-transactions target table is ready.')
