#!/usr/bin/env python3
"""** DEPRECATED - DO NOT RUN **

The GL-posting source this pipeline reads (sqls/insurance-commission.sql) was
confirmed misleading (no real policyNumber/commissionRate available from
PROFITS). The "insuranceCommission" table is now loaded exclusively from the
Bancassurance business unit's Excel file via
load_insurance_commission_from_excel.py. Running this will overwrite that
real data with the old GL-posting placeholders.
"""
"""Run the insurance-commission streaming pipeline."""
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from insurance_commission._deprecated_db2_based_insurance_commission_streaming_pipeline import InsuranceCommissionStreamingPipeline

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    InsuranceCommissionStreamingPipeline(batch_size=1000, consumer_batch_size=100).run_streaming_pipeline()
