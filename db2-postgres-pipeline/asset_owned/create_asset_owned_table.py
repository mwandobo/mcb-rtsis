#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the asset-owned pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from asset_owned.asset_owned_streaming_pipeline import AssetOwnedStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    AssetOwnedStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Asset-owned target table is ready.')
