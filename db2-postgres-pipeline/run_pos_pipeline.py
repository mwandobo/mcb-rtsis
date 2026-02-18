#!/usr/bin/env python3
"""
Runner script for POS streaming pipeline
"""

from pos_streaming_pipeline import POSStreamingPipeline

if __name__ == "__main__":
    pipeline = POSStreamingPipeline(batch_size=500)
    pipeline.run_streaming_pipeline()
