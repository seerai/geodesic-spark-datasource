package ai.seer.geodesic.sources.boson

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
import ai.seer.geodesic.GeodesicClient
import ai.seer.geodesic.FieldDef
import ai.seer.geodesic.DatasetInfo
import ai.seer.geodesic.FeatureCollection
import ai.seer.geodesic.Feature
import play.api.libs.json.JsString
import play.api.libs.json.JsNumber
import play.api.libs.json.JsBoolean
import org.apache.spark.sql.types.BinaryType
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKBWriter
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.internal.Logging

class DefaultSource extends TableProvider with Logging {
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

    // Get dataset info to determine max page size
    val client = new GeodesicClient()
    val info = client.datasetInfo(datasetId, projectId)

    // Determine the effective page size with proper precedence:
    // 1. User-provided value (if specified)
    // 2. Dataset's maxPageSize (if available)
    // 3. Default value (2000)
    val defaultPageSize = 2000
    val datasetMaxPageSize = info.config match {
      case Some(config) => config.maxPageSize
      case None         => defaultPageSize
    }

    val requestedPageSize = properties.get("pageSize") match {
      case null => datasetMaxPageSize // No user value, use dataset max
      case userValue =>
        val userPageSize = userValue.toInt
        // User value takes precedence, but warn if it exceeds dataset max and use the max
        if (userPageSize > datasetMaxPageSize) {
          logWarning(
            s"Requested pageSize ($userPageSize) exceeds dataset maxPageSize ($datasetMaxPageSize). Using max value."
          )
          datasetMaxPageSize
        } else {
          userPageSize
        }
    }

    val src = new DataSourceConfig(
      datasetId,
      projectId,
      collectionId,
      requestedPageSize
    )

    new BosonTable(src)
  }
}

case class DataSourceConfig(
    datasetId: String,
    projectId: String,
    collectionId: String,
    pageSize: Int
)

class BosonTable(
    src: DataSourceConfig
) extends Table
    with SupportsRead {

  private val client = new GeodesicClient()
  private val info = client.datasetInfo(src.datasetId, src.projectId)

  override def name(): String = this.getClass.getName

  override def schema(): StructType = {
    BosonTable.getSchema(src.collectionId, client, info)
  }

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    new BosonScanBuilder(client, src, info)
  }
}

object BosonTable {
  def apply(
      src: DataSourceConfig
  ): BosonTable = {
    new BosonTable(src)
  }

  def getSchema(
      collectionId: String,
      client: GeodesicClient,
      info: DatasetInfo
  ): StructType = {
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
    src: DataSourceConfig,
    info: DatasetInfo
) extends ScanBuilder {
  override def build(): Scan = {
    new BosonScan(client, src, info)
  }
}

case class BosonPartition(
    partitionNumber: Int,
    client: GeodesicClient,
    src: DataSourceConfig
) extends InputPartition

class BosonScan(
    client: GeodesicClient,
    src: DataSourceConfig,
    info: DatasetInfo
) extends Scan
    with Batch {
  override def readSchema(): StructType = {
    BosonTable.getSchema(src.collectionId, client, info)
  }

  override def toBatch(): Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new BosonPartition(0, client, src))
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
    extends PartitionReader[InternalRow]
    with Logging {

  var features: List[Feature] = List.empty
  var nextLink: Option[String] = None
  var index: Int = 0
  var hasInitialized: Boolean = false
  var isExhausted: Boolean = false

  // Cache the schema and field types for efficient lookup
  private val schema = BosonTable.getSchema(
    partition.src.collectionId,
    partition.client,
    partition.client
      .datasetInfo(partition.src.datasetId, partition.src.projectId)
  )
  private val fieldTypes = schema.fields.map(f => f.name -> f.dataType).toMap

  override def next(): Boolean = {
    // If we're already exhausted, return false
    if (isExhausted) {
      return false
    }

    // If we have features and haven't reached the end of current page
    if (index < features.size) {
      return true
    }

    // We've exhausted current page
    // If there's no next link, we've reached the end of the dataset
    if (nextLink.isEmpty && hasInitialized) {
      isExhausted = true
      return false
    }

    // Try to fetch next page
    fetchNextPage()
  }

  private def fetchNextPage(): Boolean = {
    try {
      val sr = partition.client.search(
        partition.src.datasetId,
        partition.src.projectId,
        partition.src.pageSize,
        if (hasInitialized) nextLink else None
      )

      hasInitialized = true
      features = sr.features
      index = 0

      // Update nextLink for subsequent calls
      nextLink = sr.links
        .find(_.rel == "next")
        .map(_.href)

      // Return true if we have features to process
      features.nonEmpty
    } catch {
      case e: Exception =>
        logError(s"Error fetching data: ${e.getMessage}", e)
        isExhausted = true
        false
    }
  }

  override def get(): InternalRow = {
    if (index >= features.size) {
      throw new Exception("No more features available - call next() first")
    }

    val feature = features(index)
    index += 1

    val tuple = feature.properties
      .map { case (key, value) =>
        // Get the expected type for this field from the schema
        val expectedType = fieldTypes.get(key)

        value match {
          case JsString(s) => Option(key, UTF8String.fromString(s.toString()))
          case JsNumber(n) =>
            expectedType match {
              case Some(IntegerType) =>
                // Convert to integer, rounding to the nearest value to handle potential decimal values
                Option(key, Math.round(n.doubleValue()).toInt)
              case Some(DoubleType) =>
                Option(key, n.doubleValue())
              case _ =>
                // Default to double if type is unknown
                Option(key, n.doubleValue())
            }
          case JsBoolean(b) => Option(key, b.booleanValue())
          case _            => None
        }
      }
      .filter(_.isDefined)
      .map(_.get)
      .toSeq
      .sortBy(_._1)

    val serializedGeometry = GeometryUDT.serialize(feature.geometry)
    val values = tuple.map(_._2) :+ serializedGeometry
    InternalRow.fromSeq(values)
  }

  override def close(): Unit = {
    features = List.empty
    isExhausted = true
  }
}
