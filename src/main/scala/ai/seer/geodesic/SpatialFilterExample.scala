package ai.seer.geodesic

import org.apache.spark.sql.SparkSession
import org.apache.sedona.spark.SedonaContext

object SpatialFilterExample extends App {

  println(
    "🚀 Creating Spark session with Sedona and spatial pushdown enabled..."
  )

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("GeodesicSedonaIntegratedSpatialFilterDemo")
    // Enable spatial filter pushdown via configuration
    .config("spark.sedona.geodesic.spatialFilterPushDown", "true")
    // Register the Geodesic Spark Extension for automatic rule registration
    .config("spark.sql.extensions", "ai.seer.geodesic.GeodesicSparkExtension")
    .getOrCreate()

  // Create Sedona context (this registers all Sedona functions)
  val sedona = SedonaContext.create(spark)

  println(
    "✅ Sedona context created with automatic spatial pushdown via Spark Extension"
  )

  // Load data from Geodesic
  val df = sedona.read
    .format("ai.seer.geodesic.sources.boson")
    .option("datasetId", "ukr-adm3-boundaries")
    .option("projectId", "global")
    .load()

  println("=== Original Dataset ===")
  println(s"Total count: ${df.count()}")
  df.show(5)

  // Create a test polygon around Kyiv area (approximate coordinates)
  val kyivBbox =
    "POLYGON((30.0 50.0, 31.0 50.0, 31.0 51.0, 30.0 51.0, 30.0 50.0))"

  println(s"\n=== Testing Spatial Filter with Kyiv Bounding Box ===")
  println(s"Bounding box WKT: $kyivBbox")

  // Create a temporary view for SQL operations
  df.createOrReplaceTempView("boundaries")

  // Test spatial intersects query
  val spatialQuery = s"""
    SELECT name, admin_level, geometry_bbox_xmin, geometry_bbox_ymin, geometry_bbox_xmax, geometry_bbox_ymax
    FROM boundaries 
    WHERE ST_Intersects(geometry, ST_GeomFromWKT('$kyivBbox'))
  """

  println("Executing spatial query with Advanced Interceptor...")
  println("*** WATCH FOR INTERCEPTOR LOGS ***")

  val startTime = System.currentTimeMillis()
  val spatialResult = sedona.sql(spatialQuery)
  val spatialCount = spatialResult.count()
  val spatialTime = System.currentTimeMillis() - startTime

  println(s"Spatial filtered count: $spatialCount")
  println(s"Spatial query time: ${spatialTime}ms")
  spatialResult.show(10)

  // Test with a point intersection
  val kyivPoint = "POINT(30.5 50.5)"
  println(s"\n=== Testing Point Intersection with Advanced Interceptor ===")
  println(s"Point WKT: $kyivPoint")
  println("*** WATCH FOR INTERCEPTOR LOGS ***")

  val pointQuery = s"""
    SELECT name, admin_level, geometry_bbox_xmin, geometry_bbox_ymin, geometry_bbox_xmax, geometry_bbox_ymax
    FROM boundaries 
    WHERE ST_Intersects(geometry, ST_GeomFromWKT('$kyivPoint'))
  """

  val pointStartTime = System.currentTimeMillis()
  val pointResult = sedona.sql(pointQuery)
  val pointCount = pointResult.count()
  val pointTime = System.currentTimeMillis() - pointStartTime

  println(s"Point intersection count: $pointCount")
  println(s"Point query time: ${pointTime}ms")
  pointResult.show(5)

  spark.stop()
}
