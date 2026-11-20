#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the other-capital-accounts pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from other_capital_accounts.other_capital_accounts_streaming_pipeline import OtherCapitalAccountsStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    OtherCapitalAccountsStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Other-capital-accounts target table is ready.')
