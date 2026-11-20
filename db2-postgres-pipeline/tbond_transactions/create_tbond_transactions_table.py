#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the tbond-transactions pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tbond_transactions.tbond_transactions_streaming_pipeline import TbondTransactionsStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    TbondTransactionsStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Tbond-transactions target table is ready.')
