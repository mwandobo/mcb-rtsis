#!/usr/bin/env python3
"""Continuous wrapper for the insuranceCommission Bancassurance Excel loader.

This file replaces the previous DB2/GLI_TRX_EXTRACT-based implementation,
which was confirmed misleading (see load_insurance_commission_from_excel.py's
module docstring) and was never registered or run via the dashboard. That
old implementation is preserved, unused, at
_deprecated_db2_based_insurance_commission_streaming_pipeline.py.

Not DB2-sourced, so there's no TMSTAMP to filter on. "Incremental" here means:
skip the reload entirely when the source spreadsheet's own last-modified time
hasn't changed since this pipeline's last successful run. When it changes,
the reload is a full replace (not a row-level incremental insert) - the
spreadsheet is the business's current source of truth, and a row-level
incremental-insert would never reflect a correction or removal made in it.
"""
import logging
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from load_insurance_commission_from_excel import load_rows, replace_table

EXCEL_PATH = r'J:\RTSIS\BANCASSUARANCE\Bankassuarance (006).xlsx'


class InsuranceCommissionPipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.records_processed = 0

    def get_last_successful_run(self):
        try:
            from pipeline_state import PipelineStateManager
            return PipelineStateManager().get_last_successful_run('insurance_commission')
        except Exception as e:
            self.logger.warning(f"Could not get last successful run: {e}")
            return None

    def run_streaming_pipeline(self, incremental=True):
        self.logger.info("Starting Insurance Commission pipeline...")
        self._update_state('running')
        try:
            if not os.path.exists(EXCEL_PATH):
                raise FileNotFoundError(f"Source spreadsheet not reachable: {EXCEL_PATH}")

            file_mtime = datetime.fromtimestamp(os.path.getmtime(EXCEL_PATH))
            last_run = self.get_last_successful_run() if incremental else None

            if incremental and last_run and file_mtime <= last_run:
                self.logger.info(
                    f"Source file unchanged since last run (mtime {file_mtime} <= last_run {last_run}), skipping reload"
                )
                self.records_processed = 0
                self._update_state('completed')
                return

            self.logger.info(f"Reloading from {EXCEL_PATH} (mtime {file_mtime})")
            rows = load_rows(EXCEL_PATH)
            replace_table(rows)
            self.records_processed = len(rows)
            self.logger.info(f"insuranceCommission replaced: {len(rows)} row(s) loaded")
            self._update_state('completed')
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            self._update_state('failed', str(e))
            raise

    def _update_state(self, status, error_message=None):
        try:
            from pipeline_state import PipelineStateManager
            PipelineStateManager().update_run('insurance_commission', status, self.records_processed, error_message)
            self.logger.info(f"State updated: {status}, records: {self.records_processed}")
        except Exception as e:
            self.logger.warning(f"Could not update state: {e}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Insurance Commission Pipeline')
    parser.add_argument('--full-load', action='store_true', help='Reload even if the source file is unchanged')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    InsuranceCommissionPipeline().run_streaming_pipeline(incremental=not args.full_load)
