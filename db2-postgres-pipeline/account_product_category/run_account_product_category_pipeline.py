#!/usr/bin/env python3
"""
Run Account Product Category Pipeline
"""
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from account_product_category_streaming_pipeline import AccountProductCategoryStreamingPipeline

if __name__ == "__main__":
    pipeline = AccountProductCategoryStreamingPipeline()
    pipeline.run_streaming_pipeline()