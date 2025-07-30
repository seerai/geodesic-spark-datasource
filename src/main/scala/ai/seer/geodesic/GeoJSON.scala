package ai.seer.geodesic

import play.api.libs.json._
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.ParseException
import org.locationtech.jts.io.geojson.{GeoJsonReader, GeoJsonWriter}

case class Link(
    href: String,
    rel: String,
    title: String
)

case class Feature(
    geometry: Geometry,
    properties: Map[String, JsValue]
)

object Feature {}

case class FeatureCollection(
    features: List[Feature],
    links: Option[List[Link]]
)

object FeatureCollection {
  implicit final val geometryFormat: Format[Geometry] = {
    val geoJsonReader = new GeoJsonReader()
    val geoJsonWriter = new GeoJsonWriter()

    new Format[Geometry] {
      def reads(json: JsValue): JsResult[Geometry] = try {
        JsSuccess(geoJsonReader.read(json.toString))
      } catch {
        case e: ParseException => JsError(s"Invalid GeoJson: ${e.getMessage}")
      }
      def writes(geometry: Geometry): JsValue =
        Json.parse(geoJsonWriter.write(geometry))
    }
  }

  implicit val linkReads = Json.reads[Link]
  implicit val featureReads = Json.reads[Feature]
  implicit val featureCollectionReads = Json.reads[FeatureCollection]
}
