#!/usr/bin/env python3
"""
Python test script for Geodesic Spark DataSource with Apache Sedona


Requirements:
- PySpark 3.3.0+
- Apache Sedona 1.7.0+
- geodesic Python package
- geodesic-spark-datasource-sedona JAR file

Setup:
1. Install required Python packages:
   pip install geodesic-api

2. Set environment variables for authentication:
   export GEODESIC_API_KEY="your-api-key-here"
   export GEODESIC_HOST="https://api.geodesic.seerai.space"  # Optional

3. Or create config file at ~/.config/geodesic/config.json

   >>> import geodesic
   >>> geodesic.authenticate()

Usage:
   python test_geodesic_pyspark.py
"""

import os
import sys
from sedona.spark import SedonaContext

# Import geodesic Python API
try:
    import geodesic
except ImportError:
    print("❌ Error: geodesic package not found. Please install it with:")
    print("   pip install geodesic-api")
    sys.exit(1)


def create_sedona_context():
    """
    Create and configure Spark session with Sedona support
    """
    print("🚀 Setting up Spark session with Sedona...")
    config = (
        SedonaContext.builder()
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-3.3_2.12:1.7.0,"
            "org.datasyslab:geotools-wrapper:1.7.0-28.5,"
            "ai.seer:geodesic-spark-datasource-sedona_2.12:0.0.3",
        )
        .config(
            "spark.jars.repositories", "https://artifacts.unidata.ucar.edu/repository/unidata-all"
        )
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )
    sedona = SedonaContext.create(config)
    print("✅ Spark session created successfully")
    return sedona


def load_geodesic_data(sedona):
    """
    Load data from Geodesic using the Spark DataSource
    """
    print("\n📊 Loading data from Geodesic...")

    try:
        ds = geodesic.get_dataset("ukr-adm3-boundaries", project="global")

        # Load data using the Geodesic Spark DataSource
        df = (
            sedona.read.format("ai.seer.geodesic.sources.boson")
            .option("datasetId", ds.name)
            .option("projectId", ds.project.uid)
            .load()
        )

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
        spatial_df = sedona.sql(
            """
            SELECT 
                *,
                ST_Area(geometry) as area,
                ST_Centroid(geometry) as centroid
            FROM boundaries
            LIMIT 10
        """
        )

        print("📊 Spatial analysis results:")
        spatial_df.select("area", "centroid").show(5, truncate=False)

        # Filter by area (example spatial query)
        large_areas = sedona.sql(
            """
            SELECT COUNT(*) as count
            FROM boundaries 
            WHERE ST_Area(geometry) > 0.001
        """
        )

        large_count = large_areas.collect()[0]["count"]
        print(f"🏞️  Features with area > 0.001: {large_count}")

        return True

    except Exception as e:
        print(f"❌ Error during analysis: {e}")
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
    sedona = create_sedona_context()

    # Load data from Geodesic
    df = load_geodesic_data(sedona)

    if df is not None:
        # Analyze the data
        analyze_data(sedona, df)

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
    if not os.getenv("GEODESIC_API_KEY") and not os.path.exists(
        os.path.expanduser("~/.config/geodesic/config.json")
    ):
        print("⚠️  Warning: No Geodesic credentials found!")
        print("   Set GEODESIC_API_KEY environment variable or create config file")
        print("   The test will continue but may fail during data loading")
        print()

    main()
