package com.seerai.geodesic

import play.api.libs.json._
import org.apache.sedona.spark.SedonaContext

import com.seerai.geodesic.sources.boson.DefaultSource
import com.seerai.geodesic.sources.boson.{
  BosonTable,
  BosonPartition,
  BosonPartitionReader
}

/** Simple app to load the geodesic config
  */
object GeodesicConfigApp extends App {
  val client = new GeodesicClient()

  val config = SedonaContext
    .builder()
    .master("local[*]")
    .appName("GeodesicDemo")
    .getOrCreate()
  val sedona = SedonaContext.create(config)

  val df = sedona.read
    .format("com.seerai.geodesic.sources.boson")
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
