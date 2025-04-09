package ai.seer.geodesic

import play.api.libs.json._

case class ClusterConfig(api_key: Option[String], host: String, name: String)

object ClusterConfig {
  implicit val clusterConfigReads = Json.reads[ClusterConfig]
}
