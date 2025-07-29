# Geodesic Spark DataSource for Apache Sedona

[![Maven Central](https://img.shields.io/maven-central/v/ai.seer/geodesic-spark-datasource-sedona_2.12.svg)](https://search.maven.org/artifact/ai.seer/geodesic-spark-datasource-sedona_2.12)
[![Build Status](https://github.com/seerai/geodesic-spark-datasource/workflows/CI/badge.svg)](https://github.com/seerai/geodesic-spark-datasource/actions)

A Spark DataSource v2 implementation for accessing geospatial data from Geodesic with seamless Apache Sedona integration.

## Features

- **Spark DataSource v2**: Native Spark SQL integration with optimized data loading
- **Apache Sedona Integration**: Built-in support for spatial operations and geometry types
- **Pagination Support**: Efficient handling of large datasets with automatic pagination
- **Authentication**: Secure access using Geodesic API keys or token-based authentication
- **Flexible Configuration**: Support for custom datasets, projects, and collections

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>ai.seer</groupId>
    <artifactId>geodesic-spark-datasource-sedona_2.12</artifactId>
    <version>0.0.1</version>
</dependency>
```

### SBT

Add the following to your `build.sbt`:

```scala
libraryDependencies += "ai.seer" %% "geodesic-spark-datasource-sedona" % "0.0.1"
```

### Gradle

Add the following to your `build.gradle`:

```gradle
implementation 'ai.seer:geodesic-spark-datasource-sedona_2.12:0.0.1'
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
Create a configuration file at `~/.config/geodesic/config.json`:

```json
{
  "active": "default",
  "clusters": [
    {
      "name": "default",
      "host": "https://api.geodesic.seerai.space",
      "api_key": "your-api-key-here"
    }
  ]
}
```

#### 3. Custom Configuration Path
```bash
export GEODESIC_CONFIG_PATH="/path/to/your/config.json"
```

### DataSource Options

| Option | Description | Default | Required |
|--------|-------------|---------|----------|
| `datasetId` | The ID of the dataset to load | - | Yes |
| `projectId` | The project containing the dataset | `"global"` | No |
| `collectionId` | The collection within the dataset | Same as `datasetId` | No |
| `pageSize` | Number of features to fetch per page | `10000` | No |

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
2. **Filtering**: Use Spark's built-in filtering capabilities to reduce data transfer
3. **Caching**: Cache frequently accessed datasets using `df.cache()`
4. **Partitioning**: Consider repartitioning large datasets for better parallelism

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

### 0.0.1 (Initial Release)
- Spark DataSource v2 implementation
- Apache Sedona integration
- Pagination support
- Authentication via API key and config file
- Comprehensive error handling
