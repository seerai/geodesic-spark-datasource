package ai.seer.geodesic

import play.api.libs.json._
import play.api.libs.json.JsonNaming.SnakeCase

case class FieldDef(
    title: String,
    `type`: String,
    format: Option[String] = None
)

case class ProviderConfig(
    providerName: String,
    url: String,
    maxPageSize: Int = 10000
)

case class DatasetInfo(
    name: String,
    alias: String,
    config: Option[ProviderConfig] = None,
    fields: Map[String, Map[String, FieldDef]] = Map.empty,
    geometryTypes: Map[String, String] = Map.empty
)

object DatasetInfo {
  implicit val jsonConfig = JsonConfiguration(
    SnakeCase
  )
  implicit val fieldDefReads = Json.reads[FieldDef]
  implicit val providerConfigReads = Json.reads[ProviderConfig]
  implicit val datasetInfoReads = Json.reads[DatasetInfo]
}
