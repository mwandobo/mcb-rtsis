#!/usr/bin/env python3
"""
Run Personal Data Streaming Pipeline
Based on personal_data_information-v3.sql
"""

from personal_data_streaming_pipeline import PersonalDataStreamingPipeline

def main():
    """Main function to run the personal data streaming pipeline"""
    
    print("=" * 60)
    print("Personal Data Streaming Pipeline")
    print("Based on: personal_data_information-v3.sql")
    print("=" * 60)
    
    # Create pipeline with batch size
    pipeline = PersonalDataStreamingPipeline(batch_size=1000)
    
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
