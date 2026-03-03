#!/usr/bin/env python3
"""
Run Agent Transactions Streaming Pipeline
"""

import logging
import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from agent_transactions_streaming_pipeline import AgentTransactionsStreamingPipeline

def main():
    """Main function to run the agent transactions pipeline"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("STARTING AGENT TRANSACTIONS STREAMING PIPELINE")
    logger.info("=" * 60)
    
    try:
        # Create and run pipeline with batch size 1000
        pipeline = AgentTransactionsStreamingPipeline(batch_size=1000)
        pipeline.run_streaming_pipeline()
        
        logger.info("=" * 60)
        logger.info("AGENT TRANSACTIONS PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        logger.error("=" * 60)
        logger.error("AGENT TRANSACTIONS PIPELINE FAILED")
        logger.error("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    main()