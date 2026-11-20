#!/usr/bin/env python3
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from directors_records.directors_records_streaming_pipeline import DirectorsRecordsStreamingPipeline
if __name__ == '__main__': DirectorsRecordsStreamingPipeline().setup_target_table()
