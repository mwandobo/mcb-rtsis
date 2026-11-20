#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the unearned-income pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unearned_income.unearned_income_streaming_pipeline import UnearnedIncomeStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    UnearnedIncomeStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Unearned-income target table is ready.')
