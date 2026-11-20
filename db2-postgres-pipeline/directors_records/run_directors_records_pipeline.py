#!/usr/bin/env python3
import logging, os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from directors_records.directors_records_streaming_pipeline import DirectorsRecordsStreamingPipeline
if __name__ == '__main__':
 logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 DirectorsRecordsStreamingPipeline().run_streaming_pipeline()
