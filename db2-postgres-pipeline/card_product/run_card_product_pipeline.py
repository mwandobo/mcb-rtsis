#!/usr/bin/env python3
"""
Run Card Product Pipeline
"""
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from card_product_streaming_pipeline import CardProductStreamingPipeline

if __name__ == "__main__":
    pipeline = CardProductStreamingPipeline()
    pipeline.run_streaming_pipeline()