package ai.seer.geodesic

import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import org.apache.sedona.spark.SedonaContext

import ai.seer.geodesic.sources.boson.DefaultSource
import ai.seer.geodesic.sources.boson.{
  BosonTable,
  BosonPartition,
  BosonPartitionReader
}

/** Simple app to load the geodesic config
  */
object GeodesicConfigApp extends App {
  val client = new GeodesicClient()

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("GeodesicDemo")
    .getOrCreate()

  // Create Sedona context
  val sedona = SedonaContext.create(spark)

  val df = sedona.read
    .format("ai.seer.geodesic.sources.boson")
    .option("datasetId", "ukr-adm3-boundaries")
    .option("projectId", "global")
    .load()
  df.show()
  /*
  var cluster = client.load()

  val info =
    client.datasetInfo(
      "ukr-adm3-boundaries",
      "global"
    )

  println(info.fields)

  val sr = client.search("ukr-adm3-boundaries", "global")

  val f = sr.features(0).geometry
  val w = new WKBWriter()
  val wkb = w.write(f)
  println(wkb)

  val x = new BosonPartition(
    0,
    client,
    "ukr-adm3-boundaries",
    "global",
    "ukr-adm3-boundaries"
  )

  var r = new BosonPartitionReader(x)
  if (r.next()) {
    println("has next")
    println(r.get())
  } else {
    println("no next")
  }
   */
}
