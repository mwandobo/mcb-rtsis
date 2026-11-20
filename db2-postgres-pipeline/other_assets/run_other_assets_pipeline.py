#!/usr/bin/env python3
"""Run the other-assets streaming pipeline."""
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from other_assets.other_assets_streaming_pipeline import OtherAssetsStreamingPipeline

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    OtherAssetsStreamingPipeline(batch_size=1000, consumer_batch_size=100).run_streaming_pipeline()
