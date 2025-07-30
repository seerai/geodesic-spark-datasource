package ai.seer.geodesic

import org.apache.spark.sql.SparkSession
import org.apache.sedona.spark.SedonaContext

object FilterExample extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("GeodesicFilterDemo")
    .getOrCreate()

  val sedona = SedonaContext.create(spark)
  import sedona.implicits._

  // Load data from Geodesic
  val df = sedona.read
    .format("ai.seer.geodesic.sources.boson")
    .option("datasetId", "ukr-adm3-boundaries")
    .option("projectId", "global")
    .option("pageSize", "1000")
    .load()

  println("=== Original Dataset ===")
  println(s"Total count: ${df.count()}")
  df.show(5)

  println("\n=== Filtered Dataset (name = 'Marfivska') ===")
  val filtered = df.filter($"name" === "Marfivska")
  println(s"Filtered count: ${filtered.count()}")
  filtered.show(5)

  println("\n=== Filtered Dataset (geometry_bbox_xmax > 30.0) ===")
  val bboxFiltered = df.filter($"geometry_bbox_xmax" > 30.0)
  println(s"Bounding box filtered count: ${bboxFiltered.count()}")
  bboxFiltered.show(5)

  spark.stop()
}
