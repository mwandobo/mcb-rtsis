#!/usr/bin/env python3
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cash_statistics.cash_statistics_streaming_pipeline import CashStatisticsStreamingPipeline

if __name__ == '__main__':
    CashStatisticsStreamingPipeline().setup_target_table()
