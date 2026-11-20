#!/usr/bin/env python3
"""Run the asset-owned streaming pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from asset_owned.asset_owned_streaming_pipeline import AssetOwnedStreamingPipeline


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    AssetOwnedStreamingPipeline(batch_size=1000, consumer_batch_size=100).run_streaming_pipeline()
