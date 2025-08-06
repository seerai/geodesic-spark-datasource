package ai.seer.geodesic.sources.boson

import java.util
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

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
  BooleanType,
  TimestampType
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConverters._
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.sources.Filter
import ai.seer.geodesic.GeodesicClient
import ai.seer.geodesic.CQL2FilterTranslator
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
    pageSize: Int,
    cql2Filter: Option[play.api.libs.json.JsValue] = None,
    intersects: Option[play.api.libs.json.JsValue] = None
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
            val format = fieldDef.format

            (fieldType, format) match {
              case ("string", Some("date-time")) =>
                StructField(name, TimestampType)
              case ("string", _)  => StructField(name, StringType)
              case ("integer", _) => StructField(name, IntegerType)
              case ("number", _)  => StructField(name, DoubleType)
              case ("boolean", _) => StructField(name, BooleanType)
              case _              => null
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
) extends ScanBuilder
    with SupportsPushDownFilters
    with Logging {

  private var _pushedFilters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    logInfo(s"Attempting to push down ${filters.length} filters")

    val analysis = CQL2FilterTranslator.analyzeFilters(filters)

    // Log what we're pushing down
    if (analysis.cql2Filter.isDefined) {
      logInfo(s"Pushing down CQL2 filter: ${analysis.cql2Filter.get}")
    }
    if (analysis.intersects.isDefined) {
      logInfo(
        s"Pushing down spatial intersects filter: ${analysis.intersects.get}"
      )
    }
    if (analysis.unsupportedFilters.nonEmpty) {
      logInfo(
        s"Returning ${analysis.unsupportedFilters.length} unsupported filters to Spark"
      )
    }

    // Store the pushed filters for use in build()
    this._pushedFilters = filters.diff(analysis.unsupportedFilters)

    // Return unsupported filters for Spark to handle
    analysis.unsupportedFilters
  }

  override def pushedFilters(): Array[Filter] = _pushedFilters

  override def build(): Scan = {
    // Analyze filters to get CQL2 parameters
    val filterAnalysis = CQL2FilterTranslator.analyzeFilters(_pushedFilters)

    // Create updated DataSourceConfig with filters
    val updatedSrc = src.copy(
      cql2Filter = filterAnalysis.cql2Filter,
      intersects = filterAnalysis.intersects
    )

    new BosonScan(client, updatedSrc, info)
  }
}

case class BosonPartition(
    partitionNumber: Int,
    client: GeodesicClient,
    src: DataSourceConfig
) extends InputPartition

class BosonScan(
    val client: GeodesicClient,
    val src: DataSourceConfig,
    val info: DatasetInfo
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

  // Helper method to parse datetime strings to microseconds since epoch
  private def parseDateTime(dateTimeString: String): Long = {
    try {
      // Try ISO 8601 format first (handles Z suffix)
      val instant = Instant.parse(dateTimeString)
      // Convert to microseconds since epoch (Spark's internal timestamp format)
      instant.getEpochSecond * 1000000L + instant.getNano / 1000L
    } catch {
      case _: DateTimeParseException =>
        try {
          // Try parsing with timezone offset (e.g., +00:00, -05:00)
          val offsetDateTime = java.time.OffsetDateTime.parse(dateTimeString)
          val instant = offsetDateTime.toInstant
          instant.getEpochSecond * 1000000L + instant.getNano / 1000L
        } catch {
          case _: DateTimeParseException =>
            try {
              // Try parsing with ZonedDateTime for more complex timezone formats
              val zonedDateTime = java.time.ZonedDateTime.parse(dateTimeString)
              val instant = zonedDateTime.toInstant
              instant.getEpochSecond * 1000000L + instant.getNano / 1000L
            } catch {
              case _: DateTimeParseException =>
                try {
                  // Try local datetime and assume UTC
                  val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
                  val localDateTime =
                    java.time.LocalDateTime.parse(dateTimeString, formatter)
                  val instant =
                    localDateTime.atZone(java.time.ZoneOffset.UTC).toInstant
                  instant.getEpochSecond * 1000000L + instant.getNano / 1000L
                } catch {
                  case e: DateTimeParseException =>
                    logWarning(
                      s"Failed to parse datetime string '$dateTimeString': ${e.getMessage}"
                    )
                    // Return null for invalid datetime strings
                    throw new IllegalArgumentException(
                      s"Invalid datetime format: $dateTimeString",
                      e
                    )
                }
            }
        }
    }
  }

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
        if (hasInitialized) nextLink else None,
        partition.src.cql2Filter,
        partition.src.intersects
      )

      hasInitialized = true
      features = sr.features
      index = 0

      // Update nextLink for subsequent calls
      nextLink = sr.links
        .getOrElse(List.empty)
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

    // Create a map of property values with proper type conversion
    val propertyValues = feature.properties
      .map { case (key, value) =>
        // Get the expected type for this field from the schema
        val expectedType = fieldTypes.get(key)

        value match {
          case JsString(s) =>
            expectedType match {
              case Some(TimestampType) =>
                try {
                  Option(key, parseDateTime(s))
                } catch {
                  case e: IllegalArgumentException =>
                    logError(
                      s"Failed to parse datetime for field '$key': ${e.getMessage}"
                    )
                    Option(
                      key,
                      null
                    ) // Return null for invalid datetime strings
                }
              case _ => Option(key, UTF8String.fromString(s.toString()))
            }
          case JsNumber(n) =>
            expectedType match {
              case Some(IntegerType) =>
                // Convert to integer, rounding to the nearest value to handle potential decimal values
                Option(key, Math.round(n.doubleValue()).toInt)
              case Some(DoubleType) =>
                Option(key, n.doubleValue())
              case Some(StringType) =>
                // Convert numeric values to strings when schema expects StringType
                Option(key, UTF8String.fromString(n.toString()))
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
      .toMap

    // Create values array in the same order as schema fields
    val values = schema.fields.map { field =>
      if (field.name == "geometry") {
        GeometryUDT.serialize(feature.geometry)
      } else {
        propertyValues.getOrElse(field.name, null)
      }
    }

    InternalRow.fromSeq(values)
  }

  override def close(): Unit = {
    features = List.empty
    isExhausted = true
  }
}
