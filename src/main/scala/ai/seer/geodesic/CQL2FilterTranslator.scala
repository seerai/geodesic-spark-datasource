package ai.seer.geodesic

import org.apache.spark.sql.sources._
import play.api.libs.json._
import org.apache.spark.internal.Logging

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

  /** Analyzes and separates filters into supported categories
    */
  def analyzeFilters(filters: Array[Filter]): FilterAnalysis = {
    val (supported, unsupported) = filters.partition(canPushDown)

    val (spatialFilters, attributeFilters) =
      supported.partition(isSpatialFilter)

    val cql2Filter = if (attributeFilters.nonEmpty) {
      Some(translateAttributeFilters(attributeFilters))
    } else None

    val intersects = if (spatialFilters.nonEmpty) {
      extractIntersectsGeometry(spatialFilters)
    } else None

    FilterAnalysis(cql2Filter, intersects, unsupported)
  }

  /** Determines if a filter can be pushed down to the server
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
    // Spatial filters (these will be handled separately)
    case f if isSpatialFilter(f) => true
    case _                       => false
  }

  /** Determines if a filter is spatial (should go to intersects parameter)
    */
  def isSpatialFilter(filter: Filter): Boolean = filter match {
    // For now, we'll detect spatial filters by function name patterns
    // This is a simplified approach - in practice you might want more sophisticated detection
    case f =>
      f.toString.toLowerCase.contains("st_intersects") ||
      f.toString.toLowerCase.contains("st_within") ||
      f.toString.toLowerCase.contains("st_contains")
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

  /** Extracts geometry for intersects parameter from spatial filters This is a
    * simplified implementation - in practice you'd need more sophisticated
    * geometry extraction from spatial function calls
    */
  def extractIntersectsGeometry(
      spatialFilters: Array[Filter]
  ): Option[JsValue] = {
    // For now, return None as we need more context about how spatial filters
    // are represented in your Spark SQL queries
    // This would need to be implemented based on how Sedona spatial functions
    // are represented in the filter tree
    logInfo(
      s"Spatial filters detected but geometry extraction not yet implemented: ${spatialFilters.mkString(", ")}"
    )
    None
  }
}
