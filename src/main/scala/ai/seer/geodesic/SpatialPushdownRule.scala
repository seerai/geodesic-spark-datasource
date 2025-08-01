package ai.seer.geodesic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.PushableColumn
import org.apache.spark.sql.execution.datasources.PushableColumnBase
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.ST_Intersects
import org.apache.spark.sql.sedona_sql.expressions.ST_Contains
import org.apache.spark.sql.sedona_sql.expressions.ST_Within
import org.apache.spark.sql.sedona_sql.expressions.ST_Overlaps
import org.apache.spark.sql.sedona_sql.expressions.ST_Touches
import org.apache.spark.sql.sedona_sql.expressions.ST_Crosses
import org.apache.spark.sql.sedona_sql.optimization.ExpressionUtils.splitConjunctivePredicates
import org.apache.spark.internal.Logging
import org.locationtech.jts.geom.Geometry
import play.api.libs.json._
import ai.seer.geodesic.sources.boson.BosonScan

/** Spatial filter pushdown rule for Geodesic data source. This rule intercepts
  * spatial expressions (ST_Intersects, ST_Contains, ST_Within, ST_Overlaps,
  * ST_Touches, ST_Crosses) and pushes them down to the Geodesic API as
  * intersects parameters for server-side processing.
  *
  * Since Sedona runs these as post-scan filters anyway, pushing them down as
  * intersects filters provides significant performance benefits.
  */
class SpatialPushdownRule(sparkSession: SparkSession)
    extends Rule[LogicalPlan]
    with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val enableSpatialFilterPushDown =
      sparkSession.conf
        .get("spark.sedona.geodesic.spatialFilterPushDown", "true")
        .toBoolean

    if (!enableSpatialFilterPushDown) {
      plan
    } else {
      plan transform {
        case filter @ Filter(condition, relation: DataSourceV2ScanRelation)
            if isGeodesicRelation(relation) =>
          val filters = splitConjunctivePredicates(condition)
          val filtersWithoutSubquery =
            filters.filterNot(SubqueryExpression.hasSubquery)

          val spatialFilters = extractSpatialFilters(filtersWithoutSubquery)
          val nonSpatialFilters =
            filtersWithoutSubquery.filterNot(isSpatialExpression)

          if (spatialFilters.nonEmpty) {
            // Convert spatial filters to GeoJSON for Geodesic API
            val intersectsGeometry =
              convertSpatialFiltersToGeoJSON(spatialFilters)

            if (intersectsGeometry.isDefined) {
              // Create new scan with spatial filter pushed down to server
              val bosonScan = relation.scan.asInstanceOf[BosonScan]
              val updatedConfig =
                bosonScan.src.copy(intersects = intersectsGeometry)
              val newScan =
                new BosonScan(bosonScan.client, updatedConfig, bosonScan.info)
              val newRelation = relation.copy(scan = newScan)

              // Return the new relation with spatial filters pushed down
              if (nonSpatialFilters.nonEmpty) {
                val combinedNonSpatialFilter = nonSpatialFilters.reduce(And)
                Filter(combinedNonSpatialFilter, newRelation)
              } else {
                newRelation
              }
            } else {
              filter
            }
          } else {
            filter
          }
      }
    }
  }

  private def isGeodesicRelation(
      relation: DataSourceV2ScanRelation
  ): Boolean = {
    relation.scan.isInstanceOf[BosonScan]
  }

  private def extractSpatialFilters(
      predicates: Seq[Expression]
  ): Seq[Expression] = {
    predicates.filter(isSpatialExpression)
  }

  private def isSpatialExpression(expr: Expression): Boolean = {
    expr match {
      case _: ST_Intersects => true
      case _: ST_Contains   => true
      case _: ST_Within     => true
      case _: ST_Overlaps   => true
      case _: ST_Touches    => true
      case _: ST_Crosses    => true
      case _                => false
    }
  }

  private def convertSpatialFiltersToGeoJSON(
      spatialFilters: Seq[Expression]
  ): Option[JsValue] = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled = false)

    // For now, handle the first spatial filter (can be extended to handle multiple)
    spatialFilters.headOption.flatMap { filter =>
      convertSpatialFilterToGeoJSON(filter, pushableColumn)
    }
  }

  private def convertSpatialFilterToGeoJSON(
      predicate: Expression,
      pushableColumn: PushableColumnBase
  ): Option[JsValue] = {
    predicate match {
      // All spatial predicates can be pushed down as intersects filters
      // since Sedona will run them as post-scan filters anyway
      case ST_Intersects(args) =>
        extractGeometryFromArgs(args, pushableColumn).flatMap(geometryToGeoJSON)
      case ST_Contains(args) =>
        extractGeometryFromArgs(args, pushableColumn).flatMap(geometryToGeoJSON)
      case ST_Within(args) =>
        extractGeometryFromArgs(args, pushableColumn).flatMap(geometryToGeoJSON)
      case ST_Overlaps(args) =>
        extractGeometryFromArgs(args, pushableColumn).flatMap(geometryToGeoJSON)
      case ST_Touches(args) =>
        extractGeometryFromArgs(args, pushableColumn).flatMap(geometryToGeoJSON)
      case ST_Crosses(args) =>
        extractGeometryFromArgs(args, pushableColumn).flatMap(geometryToGeoJSON)

      case And(left, right) =>
        // For AND conditions, try to extract spatial predicates from either side
        convertSpatialFilterToGeoJSON(left, pushableColumn)
          .orElse(convertSpatialFilterToGeoJSON(right, pushableColumn))

      case _ => None
    }
  }

  private def extractGeometryFromArgs(
      args: Seq[Expression],
      pushableColumn: PushableColumnBase
  ): Option[Geometry] = {
    args match {
      case Seq(pushableColumn(_), Literal(v, _)) =>
        Some(GeometryUDT.deserialize(v))
      case Seq(Literal(v, _), pushableColumn(_)) =>
        Some(GeometryUDT.deserialize(v))
      case _ => None
    }
  }

  private def geometryToGeoJSON(geometry: Geometry): Option[JsValue] = {
    // Convert JTS Geometry to GeoJSON
    val wkt = geometry.toText()
    CQL2FilterTranslator.wktToGeoJson(wkt)
  }
}
