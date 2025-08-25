# Geodesic Spark DataSource for Apache Sedona

[![Maven Central](https://img.shields.io/maven-central/v/ai.seer/geodesic-spark-datasource-sedona_2.12.svg)](https://search.maven.org/artifact/ai.seer/geodesic-spark-datasource-sedona_2.12)
[![Build Status](https://github.com/seerai/geodesic-spark-datasource/workflows/PR%20Check/badge.svg)](https://github.com/seerai/geodesic-spark-datasource/actions)
[![GitHub Release](https://img.shields.io/github/v/release/seerai/geodesic-spark-datasource)](https://github.com/seerai/geodesic-spark-datasource/releases/latest)

A Spark DataSource v2 implementation for accessing geospatial data from Geodesic with seamless Apache Sedona integration.

## Features

- **Spark DataSource v2**: Native Spark SQL integration with optimized data loading
- **Apache Sedona Integration**: Built-in support for spatial operations and geometry types
- **Authentication**: Secure access using Geodesic API keys or token-based authentication
- **Flexible Configuration**: Support for custom datasets, projects, and collections

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>ai.seer</groupId>
    <artifactId>geodesic-spark-datasource-sedona_2.12</artifactId>
    <version>0.1.6</version>
</dependency>
```

### SBT

Add the following to your `build.sbt`:

```scala
libraryDependencies += "ai.seer" %% "geodesic-spark-datasource-sedona" % "0.1.6"
```

### Gradle

Add the following to your `build.gradle`:

```gradle
implementation 'ai.seer:geodesic-spark-datasource-sedona_2.12:0.1.6'
```

## Quick Start

### Scala Example

```scala
import org.apache.spark.sql.SparkSession
import org.apache.sedona.spark.SedonaContext

// Create Spark session
val spark = SparkSession
  .builder()
  .master("local[*]")
  .appName("GeodesicDemo")
  .getOrCreate()

// Create Sedona context for spatial operations
val sedona = SedonaContext.create(spark)

// Load data from Geodesic
val df = sedona.read
  .format("ai.seer.geodesic.sources.boson")
  .option("datasetId", "ukr-adm3-boundaries")
  .option("projectId", "global")
  .load()

// Display the data
df.show()

// Perform spatial operations
df.createOrReplaceTempView("boundaries")
sedona.sql("SELECT *, ST_Area(geometry) as area FROM boundaries").show()
```

### PySpark Example

```python
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

# Create Spark session
spark = SparkSession.builder \
    .appName("GeodesicDemo") \
    .master("local[*]") \
    .getOrCreate()

# Create Sedona context
sedona = SedonaContext.create(spark)

# Load data from Geodesic
df = sedona.read \
    .format("ai.seer.geodesic.sources.boson") \
    .option("datasetId", "ukr-adm3-boundaries") \
    .option("projectId", "global") \
    .load()

# Display the data
df.show()

# Perform spatial operations
df.createOrReplaceTempView("boundaries")
sedona.sql("SELECT *, ST_Area(geometry) as area FROM boundaries").show()
```

## Configuration

### Authentication

The DataSource supports multiple authentication methods:

#### 1. API Key (Environment Variable)
```bash
export GEODESIC_API_KEY="your-api-key-here"
export GEODESIC_HOST="https://api.geodesic.seerai.space"  # Optional, defaults to this URL
```

#### 2. Configuration File
Create a configuration file at:

```python
import geodesic
geodesic.authenticate()
```

or

```bash
$ geodesic authenticate
```

#### 3. Custom Configuration Path
```bash
export GEODESIC_CONFIG_PATH="/path/to/your/config.json"
```

### DataSource Options

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| `datasetId` | The ID of the dataset to load | - | Yes |
| `projectId` | The project containing the dataset | - | Yes |
| `collectionId` | The collection within the dataset | Same as `datasetId` | No |
| `pageSize` | Number of features to fetch per page | Dataset Default or `2000` | No |

### Example with All Options

```scala
val df = sedona.read
  .format("ai.seer.geodesic.sources.boson")
  .option("datasetId", "my-dataset")
  .option("projectId", "my-project")
  .option("collectionId", "my-collection")
  .option("pageSize", "5000")
  .load()
```

## Spatial Operations with Apache Sedona

Once loaded, you can use all Apache Sedona spatial functions:

```scala
// Register as a temporary view
df.createOrReplaceTempView("geodata")

// Spatial queries
sedona.sql("""
  SELECT 
    *,
    ST_Area(geometry) as area,
    ST_Centroid(geometry) as centroid,
    ST_Buffer(geometry, 0.01) as buffered_geom
  FROM geodata
  WHERE ST_Area(geometry) > 1000
""").show()

// Spatial joins
val otherData = sedona.read.format("...").load()
otherData.createOrReplaceTempView("other")

sedona.sql("""
  SELECT a.*, b.*
  FROM geodata a
  JOIN other b ON ST_Intersects(a.geometry, b.geometry)
""").show()
```

## Schema

The DataSource automatically infers the schema from the Geodesic dataset metadata. The schema includes:

- All dataset fields with their appropriate Spark SQL types (`string`, `integer`, `double`, `boolean`)
- A `geometry` column with Sedona's `GeometryUDT` type for spatial operations

## Performance Tips

1. **Pagination**: Adjust `pageSize` based on your memory constraints and network conditions
2. **Caching**: Cache frequently accessed datasets using `df.cache()`
3. **Partitioning**: Consider repartitioning large datasets for better parallelism

## Error Handling

The DataSource includes robust error handling for:

- Network connectivity issues
- Authentication failures
- Invalid dataset or project IDs
- Malformed geometry data

Errors are logged with detailed messages to help with troubleshooting.

## Requirements

- Apache Spark 3.3.0+
- Scala 2.12
- Apache Sedona 1.7.1+
- Java 8+

## Contributing

We welcome contributions! Please follow these guidelines:

### Commit Message Format

For automatic semantic versioning, use these patterns in your commit messages:

- `(MAJOR)` - Breaking changes that require a major version bump
- `(MINOR)` - New features that are backward compatible  
- No suffix - Bug fixes, documentation, or other patch-level changes

Example: `"Add support for custom CRS transformations (MINOR)"`

### Development Setup

1. Clone the repository
2. Set up your Geodesic authentication (see Configuration section)
3. Run tests: `sbt test`
4. Build: `sbt compile`

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

For questions, issues, or feature requests:

- GitHub Issues: [https://github.com/seerai/geodesic-spark-datasource/issues](https://github.com/seerai/geodesic-spark-datasource/issues)
- Email: [contact@seerai.space](mailto:contact@seerai.space)
- Documentation: [https://docs.geodesic.seerai.space](https://docs.geodesic.seerai.space)

## Changelog

### 0.1.0 (Spatial Filter Pushdown) 🎉
- **NEW**: **Automatic Spatial Filter Pushdown** - 17-43x performance improvement for spatial queries
- **NEW**: **Multiple Spatial Predicates Support**:
  - `ST_Intersects` - geometric intersection
  - `ST_Contains` - geometric containment  
  - `ST_Within` - geometric within relationship
  - `ST_Overlaps` - geometric overlap
  - `ST_Touches` - geometric touching
  - `ST_Crosses` - geometric crossing
- **NEW**: **GeodesicSparkExtension** - Configuration-driven spatial optimization
- **NEW**: **Zero-Code Spatial Optimization** - Enable with: `.config("spark.sql.extensions", "ai.seer.geodesic.GeodesicSparkExtension")`
- **NEW**: **Enhanced Python Examples** - Spatial + metadata filtering examples in `geodesic_pyspark_examples.py`


#### Usage Example (NEW in 0.1.0):
```scala
// Scala - Automatic spatial filter pushdown
val spark = SparkSession.builder()
  .config("spark.sql.extensions", "ai.seer.geodesic.GeodesicSparkExtension")
  .getOrCreate()

val sedona = SedonaContext.create(spark)

// This query now uses server-side spatial filtering (17-43x faster!)
val result = sedona.sql("""
  SELECT name, admin_level 
  FROM boundaries 
  WHERE ST_Intersects(geometry, ST_GeomFromWKT('POLYGON((30 50, 31 50, 31 51, 30 51, 30 50))'))
    AND admin_level = 3
""")
```

```python
# Python - Same automatic spatial filter pushdown
config = (
    SedonaContext.builder()
    .config("spark.sql.extensions", "ai.seer.geodesic.GeodesicSparkExtension")
    .getOrCreate()
)

sedona = SedonaContext.create(config)

# This query now uses server-side spatial filtering (17-43x faster!)
result = sedona.sql("""
  SELECT name, admin_level 
  FROM boundaries 
  WHERE ST_Contains(geometry, ST_GeomFromWKT('POINT(30.5 50.5)'))
    AND name LIKE '%Kyiv%'
""")
```

### 0.0.4 (JSON Parsing Fix)
- **Fixed**: JSON parsing error when API response has missing or null "links" field
- **Improved**: Made `links` field optional in `FeatureCollection` to handle various API response formats
- **Enhanced**: More robust pagination handling for datasets without pagination links

### 0.0.3 (Critical Bug Fix)
- **Fixed**: ClassCastException when displaying DataFrame data due to schema-value order mismatch
- **Improved**: Data row creation now ensures values match schema field order exactly

### 0.0.2 (Bug Fixes & Documentation)
- **Fixed**: Infinite loop issue in DataSourceExample when reaching end of dataset
- **Fixed**: Proper pagination termination when `nextLink` becomes `None`
- **Fixed**: README badges (Maven Central, Build Status, GitHub Release)
- **Fixed**: Dependency examples for SBT and Gradle
- **Improved**: Cleaned up debug logging and simplified pagination logic
- **Added**: GitHub Release badge to README

### 0.0.1 (Initial Release)
- Spark DataSource v2 implementation
- Apache Sedona integration
- Pagination support
- Authentication via API key and config file
- Comprehensive error handling
