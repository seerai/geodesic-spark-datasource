package ai.seer.geodesic

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.internal.Logging

/** Spark Extension that automatically registers the Geodesic spatial filter
  * pushdown rule. This extension is automatically loaded when the JAR is in the
  * classpath and the spark.sql.extensions configuration includes this
  * extension.
  *
  * Usage:
  *   - Scala: .config("spark.sql.extensions",
  *     "ai.seer.geodesic.GeodesicSparkExtension")
  *   - Python: .config("spark.sql.extensions",
  *     "ai.seer.geodesic.GeodesicSparkExtension")
  *
  * This makes spatial pushdown work automatically in both Scala and Python!
  */
class GeodesicSparkExtension
    extends (SparkSessionExtensions => Unit)
    with Logging {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    logInfo(
      "GeodesicSparkExtension: Registering Geodesic spatial filter pushdown rule"
    )

    // Inject an optimizer rule that automatically adds itself to extraOptimizations
    extensions.injectOptimizerRule { sparkSession =>
      val enableSpatialFilterPushDown = sparkSession.conf
        .get("spark.sedona.geodesic.spatialFilterPushDown", "true")
        .toBoolean

      if (enableSpatialFilterPushDown) {
        logInfo(
          "GeodesicSparkExtension: Spatial filter pushdown enabled - adding to extraOptimizations"
        )

        // Create the spatial filter pushdown rule
        val rule = new SpatialPushdownRule(sparkSession)

        // Add it to extraOptimizations automatically
        val currentOptimizations = sparkSession.experimental.extraOptimizations
        sparkSession.experimental.extraOptimizations =
          currentOptimizations :+ rule

        logInfo(
          "GeodesicSparkExtension: Successfully added spatial filter pushdown rule to extraOptimizations"
        )

        // Return a no-op rule since the real rule is now in extraOptimizations
        new org.apache.spark.sql.catalyst.rules.Rule[
          org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
        ] {
          override def apply(
              plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
          ) = plan
        }
      } else {
        logInfo(
          "GeodesicSparkExtension: Spatial filter pushdown disabled via configuration"
        )
        // Return a no-op rule
        new org.apache.spark.sql.catalyst.rules.Rule[
          org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
        ] {
          override def apply(
              plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
          ) = plan
        }
      }
    }

    logInfo(
      "GeodesicSparkExtension: Successfully registered Geodesic spatial filter pushdown extension"
    )
  }
}
