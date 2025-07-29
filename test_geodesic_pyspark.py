#!/usr/bin/env python3
"""
Python test script for Geodesic Spark DataSource with Apache Sedona

This script mirrors the functionality of DataSourceExample.scala and demonstrates
how to use the Geodesic Spark DataSource from PySpark with Apache Sedona.

Requirements:
- PySpark 3.3.0+
- Apache Sedona 1.7.1+
- geodesic Python package
- geodesic-spark-datasource-sedona JAR file

Setup:
1. Install required Python packages:
   pip install pyspark apache-sedona geodesic

2. Set environment variables for authentication:
   export GEODESIC_API_KEY="your-api-key-here"
   export GEODESIC_HOST="https://api.geodesic.seerai.space"  # Optional

3. Or create config file at ~/.config/geodesic/config.json

Usage:
   python test_geodesic_pyspark.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sedona.spark import SedonaContext
from sedona.sql import st_functions as ST

# Import geodesic Python API
try:
    import geodesic
except ImportError:
    print("❌ Error: geodesic package not found. Please install it with:")
    print("   pip install geodesic")
    sys.exit(1)


def setup_spark_session():
    """
    Create and configure Spark session with Sedona support
    """
    print("🚀 Setting up Spark session with Sedona...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("GeodesicPySparkDemo") \
        .master("local[*]") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator") \
        .getOrCreate()
    
    # Create Sedona context
    sedona = SedonaContext.create(spark)
    
    print("✅ Spark session created successfully")
    return sedona


def test_geodesic_authentication():
    """
    Test Geodesic authentication and dataset access
    """
    print("\n🔐 Testing Geodesic authentication...")
    
    try:
        # Get dataset info using geodesic Python API
        # Note: Replace with actual geodesic API calls when available
        dataset_info = geodesic.get_dataset("ukr-adm3-boundaries")
        print(f"✅ Successfully authenticated and retrieved dataset info")
        print(f"   Dataset: {dataset_info.get('name', 'ukr-adm3-boundaries')}")
        return True
    except Exception as e:
        print(f"⚠️  Geodesic authentication test skipped: {e}")
        print("   This is expected if geodesic.get_dataset() is not yet implemented")
        return False


def load_geodesic_data(sedona):
    """
    Load data from Geodesic using the Spark DataSource
    """
    print("\n📊 Loading data from Geodesic...")
    
    try:
        # Load data using the Geodesic Spark DataSource
        df = sedona.read \
            .format("ai.seer.geodesic.sources.boson") \
            .option("datasetId", "ukr-adm3-boundaries") \
            .option("projectId", "global") \
            .option("pageSize", "1000") \
            .load()
        
        print("✅ Data loaded successfully")
        return df
    
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        print("\nTroubleshooting tips:")
        print("1. Ensure the geodesic-spark-datasource-sedona JAR is in the classpath")
        print("2. Check your Geodesic authentication credentials")
        print("3. Verify the dataset ID and project ID are correct")
        return None


def analyze_data(sedona, df):
    """
    Perform basic data analysis and spatial operations
    """
    print("\n🔍 Analyzing loaded data...")
    
    try:
        # Show basic info
        print(f"📈 Dataset shape: {df.count()} rows, {len(df.columns)} columns")
        
        # Display schema
        print("\n📋 Schema:")
        df.printSchema()
        
        # Show first few rows
        print("\n📄 Sample data:")
        df.show(5, truncate=False)
        
        # Register as temporary view for SQL operations
        df.createOrReplaceTempView("boundaries")
        
        # Perform spatial operations using Sedona SQL functions
        print("\n🗺️  Performing spatial operations...")
        
        # Calculate area of geometries
        spatial_df = sedona.sql("""
            SELECT 
                *,
                ST_Area(geometry) as area,
                ST_Centroid(geometry) as centroid
            FROM boundaries
            LIMIT 10
        """)
        
        print("📊 Spatial analysis results:")
        spatial_df.select("area", "centroid").show(5, truncate=False)
        
        # Filter by area (example spatial query)
        large_areas = sedona.sql("""
            SELECT COUNT(*) as count
            FROM boundaries 
            WHERE ST_Area(geometry) > 0.001
        """)
        
        large_count = large_areas.collect()[0]['count']
        print(f"🏞️  Features with area > 0.001: {large_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        return False


def demonstrate_spatial_joins(sedona):
    """
    Demonstrate spatial join capabilities (example with mock data)
    """
    print("\n🔗 Demonstrating spatial join capabilities...")
    
    try:
        # This is a placeholder for spatial join demonstration
        # In a real scenario, you would load another spatial dataset
        print("ℹ️  Spatial join example:")
        print("   # Load another spatial dataset")
        print("   other_df = sedona.read.format('...').load()")
        print("   ")
        print("   # Perform spatial join")
        print("   joined = sedona.sql('''")
        print("       SELECT a.*, b.*")
        print("       FROM boundaries a")
        print("       JOIN other_dataset b ON ST_Intersects(a.geometry, b.geometry)")
        print("   ''')")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in spatial join demo: {e}")
        return False


def performance_tips():
    """
    Display performance optimization tips
    """
    print("\n⚡ Performance Tips:")
    print("1. 📦 Adjust pageSize based on your memory and network conditions")
    print("2. 🗂️  Use df.cache() for frequently accessed datasets")
    print("3. 🔄 Consider repartitioning large datasets: df.repartition(num_partitions)")
    print("4. 🎯 Apply filters early to reduce data transfer")
    print("5. 📊 Use Spark UI to monitor performance: http://localhost:4040")


def main():
    """
    Main test function
    """
    print("🌍 Geodesic PySpark DataSource Test")
    print("=" * 50)
    
    # Setup Spark session
    sedona = setup_spark_session()
    
    # Test authentication (optional, may not be implemented yet)
    auth_success = test_geodesic_authentication()
    
    # Load data from Geodesic
    df = load_geodesic_data(sedona)
    
    if df is not None:
        # Analyze the data
        analysis_success = analyze_data(sedona, df)
        
        if analysis_success:
            # Demonstrate spatial joins
            demonstrate_spatial_joins(sedona)
            
            # Show performance tips
            performance_tips()
            
            print("\n✅ Test completed successfully!")
        else:
            print("\n⚠️  Test completed with analysis errors")
    else:
        print("\n❌ Test failed - could not load data")
        print("\nNext steps:")
        print("1. Verify your Geodesic credentials are set up correctly")
        print("2. Ensure the geodesic-spark-datasource-sedona JAR is available")
        print("3. Check network connectivity to Geodesic API")
    
    # Stop Spark session
    sedona.stop()
    print("\n🛑 Spark session stopped")


if __name__ == "__main__":
    # Configuration section - users can modify these values
    print("🔧 Configuration:")
    print(f"   GEODESIC_API_KEY: {'✅ Set' if os.getenv('GEODESIC_API_KEY') else '❌ Not set'}")
    print(f"   GEODESIC_HOST: {os.getenv('GEODESIC_HOST', 'https://api.geodesic.seerai.space')}")
    print()
    
    # Check for required environment or config
    if not os.getenv('GEODESIC_API_KEY') and not os.path.exists(os.path.expanduser('~/.config/geodesic/config.json')):
        print("⚠️  Warning: No Geodesic credentials found!")
        print("   Set GEODESIC_API_KEY environment variable or create config file")
        print("   The test will continue but may fail during data loading")
        print()
    
    main()
