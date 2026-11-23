#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the insurance-commission pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from insurance_commission.load_insurance_commission_from_excel import setup_target_table


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    setup_target_table()
    logging.getLogger(__name__).info('Insurance-commission target table is ready.')
