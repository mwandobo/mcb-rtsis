#!/usr/bin/env python3
"""Create or align the PostgreSQL target table for the other-assets pipeline."""

import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from other_assets.other_assets_streaming_pipeline import OtherAssetsStreamingPipeline


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    OtherAssetsStreamingPipeline().setup_target_table()
    logging.getLogger(__name__).info('Other-assets target table is ready.')
