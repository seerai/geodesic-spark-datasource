package ai.seer.geodesic

import org.apache.spark.sql.sources._
import play.api.libs.json._
import org.apache.spark.internal.Logging
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{WKTReader, WKTWriter}
import scala.util.{Try, Success, Failure}

/** Translates Spark SQL filters to CQL2 JSON format and extracts spatial
  * filters for the Geodesic API's intersects parameter.
  */
object CQL2FilterTranslator extends Logging {

  /** Result of filter analysis containing separated filters
    */
  case class FilterAnalysis(
      cql2Filter: Option[JsValue],
      intersects: Option[JsValue],
      unsupportedFilters: Array[Filter]
  )

  /** Analyzes and separates filters into supported categories Note: Spatial
    * filters are handled by SpatialFilterPushDown
    */
  def analyzeFilters(filters: Array[Filter]): FilterAnalysis = {
    val (supported, unsupported) = filters.partition(canPushDown)
    val cql2Filter = if (supported.nonEmpty) {
      Some(translateAttributeFilters(supported))
    } else None

    FilterAnalysis(cql2Filter, None, unsupported)
  }

  /** Determines if a filter can be pushed down to the server Note: Only
    * attribute filters are handled here now.
    */
  def canPushDown(filter: Filter): Boolean = filter match {
    case _: EqualTo            => true
    case _: EqualNullSafe      => true
    case _: GreaterThan        => true
    case _: GreaterThanOrEqual => true
    case _: LessThan           => true
    case _: LessThanOrEqual    => true
    case _: In                 => true
    case _: IsNull             => true
    case _: IsNotNull          => true
    case _: StringStartsWith   => true
    case _: StringEndsWith     => true
    case _: StringContains     => true
    case _: And                => true
    case _: Or                 => true
    case _: Not                => true
    case _                     => false
  }

  /** Translates attribute filters to CQL2 JSON format
    */
  def translateAttributeFilters(filters: Array[Filter]): JsValue = {
    if (filters.length == 1) {
      translateSingleFilter(filters(0))
    } else {
      // Multiple filters - combine with AND
      Json.obj(
        "op" -> "and",
        "args" -> JsArray(filters.map(translateSingleFilter))
      )
    }
  }

  /** Translates a single filter to CQL2 JSON
    */
  def translateSingleFilter(filter: Filter): JsValue = filter match {
    case EqualTo(attribute, value) =>
      Json.obj(
        "op" -> "=",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            valueToJson(value)
          )
        )
      )

    case EqualNullSafe(attribute, value) =>
      Json.obj(
        "op" -> "=",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            valueToJson(value)
          )
        )
      )

    case GreaterThan(attribute, value) =>
      Json.obj(
        "op" -> ">",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            valueToJson(value)
          )
        )
      )

    case GreaterThanOrEqual(attribute, value) =>
      Json.obj(
        "op" -> ">=",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            valueToJson(value)
          )
        )
      )

    case LessThan(attribute, value) =>
      Json.obj(
        "op" -> "<",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            valueToJson(value)
          )
        )
      )

    case LessThanOrEqual(attribute, value) =>
      Json.obj(
        "op" -> "<=",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            valueToJson(value)
          )
        )
      )

    case In(attribute, values) =>
      Json.obj(
        "op" -> "in",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            JsArray(values.map(valueToJson))
          )
        )
      )

    case IsNull(attribute) =>
      Json.obj(
        "op" -> "isNull",
        "args" -> JsArray(Seq(Json.obj("property" -> attribute)))
      )

    case IsNotNull(attribute) =>
      Json.obj(
        "op" -> "not",
        "args" -> JsArray(
          Seq(
            Json.obj(
              "op" -> "isNull",
              "args" -> JsArray(Seq(Json.obj("property" -> attribute)))
            )
          )
        )
      )

    case StringStartsWith(attribute, value) =>
      Json.obj(
        "op" -> "like",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            JsString(s"$value%")
          )
        )
      )

    case StringEndsWith(attribute, value) =>
      Json.obj(
        "op" -> "like",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            JsString(s"%$value")
          )
        )
      )

    case StringContains(attribute, value) =>
      Json.obj(
        "op" -> "like",
        "args" -> JsArray(
          Seq(
            Json.obj("property" -> attribute),
            JsString(s"%$value%")
          )
        )
      )

    case And(left, right) =>
      Json.obj(
        "op" -> "and",
        "args" -> JsArray(
          Seq(
            translateSingleFilter(left),
            translateSingleFilter(right)
          )
        )
      )

    case Or(left, right) =>
      Json.obj(
        "op" -> "or",
        "args" -> JsArray(
          Seq(
            translateSingleFilter(left),
            translateSingleFilter(right)
          )
        )
      )

    case Not(child) =>
      Json.obj(
        "op" -> "not",
        "args" -> JsArray(Seq(translateSingleFilter(child)))
      )

    case _ =>
      logWarning(s"Unsupported filter for CQL2 translation: $filter")
      Json.obj(
        "op" -> "=",
        "args" -> JsArray(Seq(JsBoolean(true), JsBoolean(true)))
      )
  }

  /** Converts a Scala value to JSON
    */
  def valueToJson(value: Any): JsValue = value match {
    case s: String  => JsString(s)
    case i: Int     => JsNumber(i)
    case l: Long    => JsNumber(l)
    case f: Float   => JsNumber(f.toDouble)
    case d: Double  => JsNumber(d)
    case b: Boolean => JsBoolean(b)
    case null       => JsNull
    case _          => JsString(value.toString)
  }

  /** Converts WKT to GeoJSON
    */
  def wktToGeoJson(wkt: String): Option[JsValue] = {
    try {
      val reader = new WKTReader()
      val geometry = reader.read(wkt)
      Some(geometryToGeoJson(geometry))
    } catch {
      case e: Exception =>
        logWarning(s"Failed to parse WKT: $wkt", e)
        None
    }
  }

  /** Converts JTS Geometry to GeoJSON
    */
  private def geometryToGeoJson(geometry: Geometry): JsValue = {
    val geometryType = geometry.getGeometryType
    val coordinates = geometry.getCoordinates

    geometryType.toUpperCase match {
      case "POINT" =>
        val coord = coordinates(0)
        Json.obj(
          "type" -> "Point",
          "coordinates" -> JsArray(Seq(JsNumber(coord.x), JsNumber(coord.y)))
        )

      case "POLYGON" =>
        val coordsJson = coordinates.map { coord =>
          JsArray(Seq(JsNumber(coord.x), JsNumber(coord.y)))
        }
        Json.obj(
          "type" -> "Polygon",
          "coordinates" -> JsArray(Seq(JsArray(coordsJson)))
        )

      case _ =>
        // For other geometry types, convert coordinates to a simple polygon
        val coordsJson = coordinates.map { coord =>
          JsArray(Seq(JsNumber(coord.x), JsNumber(coord.y)))
        }
        Json.obj(
          "type" -> "Polygon",
          "coordinates" -> JsArray(Seq(JsArray(coordsJson)))
        )
    }
  }
}
