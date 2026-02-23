#!/usr/bin/env python3
"""
Compare Personal Data Pipelines - Original v3 vs Optimized CTEs
"""

import time
import sys
from personal_data_streaming_pipeline import PersonalDataStreamingPipeline
from personal_data_streaming_pipeline_optimized import PersonalDataOptimizedStreamingPipeline

def clear_personal_data_table():
    """Clear the personalData table before testing"""
    import psycopg2
    from config import Config
    
    config = Config()
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        cursor = conn.cursor()
        cursor.execute('DELETE FROM "personalData"')
        conn.commit()
        conn.close()
        print("🧹 Cleared personalData table")
    except Exception as e:
        print(f"❌ Failed to clear table: {e}")

def test_pipeline(pipeline_name, pipeline_class, batch_size=50):
    """Test a pipeline and return performance metrics"""
    print(f"\n🧪 TESTING {pipeline_name}")
    print("=" * 70)
    
    # Clear table before test
    clear_personal_data_table()
    
    # Create pipeline
    pipeline = pipeline_class(batch_size)
    
    # Record start time
    start_time = time.time()
    
    try:
        # Run pipeline
        count = pipeline.run_streaming_pipeline()
        
        # Calculate metrics
        end_time = time.time()
        duration = end_time - start_time
        records_per_second = count / duration if duration > 0 else 0
        
        print(f"\n✅ {pipeline_name} COMPLETED!")
        print(f"📊 Results:")
        print(f"   Records processed: {count:,}")
        print(f"   Duration: {duration:.2f} seconds")
        print(f"   Speed: {records_per_second:.2f} records/second")
        print("=" * 70)
        
        return {
            'name': pipeline_name,
            'count': count,
            'duration': duration,
            'speed': records_per_second,
            'success': True
        }
        
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n❌ {pipeline_name} FAILED!")
        print(f"Error: {e}")
        print(f"Duration before failure: {duration:.2f} seconds")
        print("=" * 70)
        
        return {
            'name': pipeline_name,
            'count': 0,
            'duration': duration,
            'speed': 0,
            'success': False,
            'error': str(e)
        }

def main():
    """Compare both pipelines"""
    print("🏁 PERSONAL DATA PIPELINE COMPARISON")
    print("=" * 70)
    print("📋 Comparison Setup:")
    print("  - Testing both pipelines with same batch size")
    print("  - Measuring speed and accuracy")
    print("  - Winner will be selected as production pipeline")
    print("=" * 70)
    
    # Test configuration
    test_batch_size = 50  # Small batch for fair comparison
    
    # Test both pipelines
    results = []
    
    # Test 1: Original v3 Pipeline (with inlined subqueries)
    result1 = test_pipeline(
        "ORIGINAL V3 PIPELINE (Inlined Subqueries)", 
        PersonalDataStreamingPipeline, 
        test_batch_size
    )
    results.append(result1)
    
    # Wait between tests
    time.sleep(5)
    
    # Test 2: Optimized Pipeline (with CTEs)
    result2 = test_pipeline(
        "OPTIMIZED PIPELINE (CTEs)", 
        PersonalDataOptimizedStreamingPipeline, 
        test_batch_size
    )
    results.append(result2)
    
    # Compare results
    print("\n🏆 PIPELINE COMPARISON RESULTS")
    print("=" * 70)
    
    for result in results:
        status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
        print(f"{result['name']}: {status}")
        if result['success']:
            print(f"  Records: {result['count']:,}")
            print(f"  Duration: {result['duration']:.2f}s")
            print(f"  Speed: {result['speed']:.2f} records/sec")
        else:
            print(f"  Error: {result.get('error', 'Unknown error')}")
        print()
    
    # Determine winner
    successful_results = [r for r in results if r['success']]
    
    if len(successful_results) == 0:
        print("❌ NO WINNER - Both pipelines failed")
    elif len(successful_results) == 1:
        winner = successful_results[0]
        print(f"🏆 WINNER: {winner['name']}")
        print(f"   Reason: Only successful pipeline")
    else:
        # Compare speeds
        winner = max(successful_results, key=lambda x: x['speed'])
        print(f"🏆 WINNER: {winner['name']}")
        print(f"   Speed: {winner['speed']:.2f} records/sec")
        print(f"   Performance advantage: {((winner['speed'] / min(r['speed'] for r in successful_results)) - 1) * 100:.1f}% faster")
    
    print("\n📋 RECOMMENDATION:")
    if len(successful_results) > 0:
        winner = max(successful_results, key=lambda x: x['speed'])
        if "OPTIMIZED" in winner['name']:
            print("✅ Use the OPTIMIZED pipeline (personal_data_streaming_pipeline_optimized.py)")
            print("   - Better performance with CTEs")
            print("   - More maintainable query structure")
            print("   - Same data accuracy as original")
        else:
            print("✅ Use the ORIGINAL pipeline (personal_data_streaming_pipeline.py)")
            print("   - Proven performance")
            print("   - Simpler query structure")
    else:
        print("❌ Both pipelines need debugging before production use")
    
    print("=" * 70)

if __name__ == "__main__":
    main()