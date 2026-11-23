#!/usr/bin/env python3
"""Continuous wrapper for the insuranceClaim Bancassurance Excel loader.

Not DB2-sourced (see load_insurance_claim_from_excel.py's module docstring
for why), so there's no TMSTAMP to filter on. "Incremental" here means: skip
the reload entirely when the source spreadsheet's own last-modified time
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

from load_insurance_claim_from_excel import load_rows, replace_table

EXCEL_PATH = r'J:\RTSIS\BANCASSUARANCE\Bankassuarance (006).xlsx'


class InsuranceClaimPipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.records_processed = 0

    def get_last_successful_run(self):
        try:
            from pipeline_state import PipelineStateManager
            return PipelineStateManager().get_last_successful_run('insurance_claim')
        except Exception as e:
            self.logger.warning(f"Could not get last successful run: {e}")
            return None

    def run_streaming_pipeline(self, incremental=True):
        self.logger.info("Starting Insurance Claim pipeline...")
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
            self.logger.info(f"insuranceClaim replaced: {len(rows)} row(s) loaded")
            self._update_state('completed')
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            self._update_state('failed', str(e))
            raise

    def _update_state(self, status, error_message=None):
        try:
            from pipeline_state import PipelineStateManager
            PipelineStateManager().update_run('insurance_claim', status, self.records_processed, error_message)
            self.logger.info(f"State updated: {status}, records: {self.records_processed}")
        except Exception as e:
            self.logger.warning(f"Could not update state: {e}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Insurance Claim Pipeline')
    parser.add_argument('--full-load', action='store_true', help='Reload even if the source file is unchanged')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    InsuranceClaimPipeline().run_streaming_pipeline(incremental=not args.full_load)
