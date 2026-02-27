#!/usr/bin/env python3
"""
Run Card Streaming Pipeline
Based on card_information.sql
"""

from card_streaming_pipeline import CardStreamingPipeline

def main():
    """Main function to run the card streaming pipeline"""
    
    print("=" * 60)
    print("Card Streaming Pipeline")
    print("Based on: card_information.sql")
    print("=" * 60)
    
    # Create pipeline with batch size
    pipeline = CardStreamingPipeline(batch_size=1000)
    
    try:
        # Run the streaming pipeline
        pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\nPipeline stopped by user (Ctrl+C)")
    except Exception as e:
        print(f"\n\nPipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
