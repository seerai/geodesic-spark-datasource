package com.seerai.geodesic

import play.api.libs.json._

case class FieldDef(
    title: String,
    `type`: String,
    format: Option[String] = None
)

case class DatasetInfo(
    name: String,
    alias: String,
    fields: Map[String, Map[String, FieldDef]] = Map.empty
)

object DatasetInfo {
  implicit val fieldDefReads = Json.reads[FieldDef]
  implicit val datasetInfoReads = Json.reads[DatasetInfo]
}
