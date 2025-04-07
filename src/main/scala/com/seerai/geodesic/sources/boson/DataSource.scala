package com.seerai.geodesic.sources.boson

import java.util

import org.apache.spark.sql.connector.catalog.{
  TableProvider,
  Table,
  TableCapability
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{
  StringType,
  StructField,
  StructType,
  IntegerType,
  DoubleType,
  BooleanType
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConverters._
import org.apache.spark.sql.connector.catalog.SupportsRead
import com.seerai.geodesic.GeodesicClient
import com.seerai.geodesic.FieldDef
import com.seerai.geodesic.DatasetInfo
import com.seerai.geodesic.FeatureCollection
import com.seerai.geodesic.Feature
import play.api.libs.json.JsString
import play.api.libs.json.JsNumber
import play.api.libs.json.JsBoolean
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT

class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(
      null,
      Array.empty[Transform],
      options.asCaseSensitiveMap()
    ).schema()
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {

    val datasetId =
      properties.get("datasetId")
    val projectId =
      properties.getOrDefault("projectId", "global")
    val collectionId =
      properties.getOrDefault("collectionId", datasetId)

    new BosonTable(datasetId, projectId, collectionId)
  }
}

class BosonTable(
    datasetId: String,
    projectId: String,
    collectionId: String
) extends Table
    with SupportsRead {

  private val client = new GeodesicClient()
  private val info = client.datasetInfo(datasetId, projectId)

  override def name(): String = this.getClass.getName

  override def schema(): StructType = {
    BosonTable.getSchema(collectionId, client, info)
  }

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    new BosonScanBuilder(client, datasetId, projectId, collectionId, info)
  }
}

object BosonTable {
  def apply(
      datasetId: String,
      projectId: String,
      collectionId: String
  ): BosonTable = {
    new BosonTable(datasetId, projectId, collectionId)
  }

  def getSchema(
      collectionId: String,
      client: GeodesicClient,
      info: DatasetInfo
  ): StructType = {
    println("getSchema")
    println("cid", collectionId)
    println("fields", info.fields)
    var fields =
      info.fields
        .getOrElse(collectionId, Map[String, FieldDef]())
        .map {
          case (name: String, fieldDef: FieldDef) => {
            val fieldType = fieldDef.`type`
            fieldType match {
              case "string"  => StructField(name, StringType)
              case "integer" => StructField(name, IntegerType)
              case "number"  => StructField(name, DoubleType)
              case "boolean" => StructField(name, BooleanType)
              case _         => null
            }
          }
        }
        .filter(_ != null)
        .toSeq
    fields = fields.sortBy(_.name)

    fields = fields :+ StructField(
      "geometry",
      GeometryUDT
    )
    StructType(fields)
  }
}

class BosonScanBuilder(
    client: GeodesicClient,
    datasetId: String,
    projectId: String,
    collectionId: String,
    info: DatasetInfo
) extends ScanBuilder {
  override def build(): Scan = {
    new BosonScan(client, datasetId, projectId, collectionId, info)
  }
}

case class BosonPartition(
    partitionNumber: Int,
    client: GeodesicClient,
    datasetId: String,
    projectId: String,
    collectionId: String
) extends InputPartition

class BosonScan(
    client: GeodesicClient,
    datasetId: String,
    projectId: String,
    collectionId: String,
    info: DatasetInfo
) extends Scan
    with Batch {
  override def readSchema(): StructType = {
    BosonTable.getSchema(collectionId, client, info)
  }

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new BosonPartition(0, client, datasetId, projectId, collectionId))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new BosonPartitionReaderFactory()
  }
}

class BosonPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    new BosonPartitionReader(partition.asInstanceOf[BosonPartition])
  }
}

class BosonPartitionReader(partition: BosonPartition)
    extends PartitionReader[InternalRow] {

  var features: Option[List[Feature]] = None
  var nextLink: Option[String] = None
  var index: Int = 0
  override def next(): Boolean = {
    features match {
      case Some(features) =>
        if (index < features.size) {
          return true
        }
      case None =>
        val sr = partition.client.search(
          partition.datasetId,
          partition.projectId,
          nextLink
        )
        features = Option(sr.features)
        val nextLinks = sr.links
          .find(_.rel == "next")
          .map(_.href)

        if (nextLinks.isEmpty) {
          nextLink = None
          return false
        }
        nextLinks
          .foreach { link =>
            nextLink = Option(link)
          }
        index = 0
    }

    features match {
      case Some(feats) =>
        if (index < feats.size) {
          return true
        }
        features = None
        return next()
      case None =>
        // No more features to read
        return false
    }
  }

  override def get(): InternalRow = {
    var feature: Feature = null
    features match {
      case Some(features) =>
        feature = features(index)
      case None =>
        throw new Exception("No features available")
    }

    val tuple = feature.properties
      .map { case (key, value) =>
        value match {
          case JsString(s)  => Option(key, UTF8String.fromString(s.toString()))
          case JsNumber(n)  => Option(key, n.doubleValue())
          case JsBoolean(b) => Option(key, b.booleanValue())
          case _            => None
        }
      }
      .filter(_.isDefined)
      .map(_.get)
      .toSeq
      .sortBy(_._1)
    var values = tuple.map(_._2) :+ GeometryUDT.serialize(feature.geometry)
    index += 1

    InternalRow.fromSeq(values)
  }

  override def close(): Unit = {}
}
