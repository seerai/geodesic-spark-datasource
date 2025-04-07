package com.seerai.geodesic

import play.api.libs.json._


/**
  * The config class for Geodesic. Either checks environment for GEODESIC_API_KEY and optionally GEODESIC_HOST
  * or reads the active cluster from the config file located at ~/.config/geodesic/config.json
  */
case class GeodesicConfig(active: String, clusters: List[ClusterConfig])

object GeodesicConfig {
  implicit val geodesicConfigReads = Json.reads[GeodesicConfig]
}