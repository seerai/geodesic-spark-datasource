package ai.seer.geodesic

import play.api.libs.json._
import org.apache.spark.internal.Logging
import sttp.client3.{basicRequest, UriContext, HttpClientSyncBackend}
import java.io.Serializable
import sttp.client3.{Request, Response}
import java.net.http.HttpClient
import java.time.Duration
case class Tokens(access_token: String, id_token: String)

object Tokens {
  implicit val tokensReads = Json.reads[Tokens]
}

class GeodesicClient(accessToken: String = "", idToken: String = "")
    extends Serializable
    with Logging {
  private var _accessToken = accessToken
  private var _idToken = idToken
  private var _cluster: ClusterConfig = _

  /** Create an HTTP client backend with compression enabled (gzip and brotli)
    */
  private def createBackendWithCompression() = {
    val httpClient = HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofSeconds(60))
      .build()

    HttpClientSyncBackend.usingClient(httpClient)
  }

  /** Load the config from the config file or environment variables
    */
  def load(): ClusterConfig = {
    val defaultConfigPath =
      System.getProperty("user.home") + "/.config/geodesic/config.json"
    var configPath =
      sys.env.getOrElse("GEODESIC_CONFIG_PATH", defaultConfigPath)
    val apiKey = sys.env.getOrElse("GEODESIC_API_KEY", "")
    val host =
      sys.env.getOrElse("GEODESIC_HOST", "https://api.geodesic.seerai.space")

    if (!apiKey.isEmpty) {
      return ClusterConfig(Some(apiKey), host, "")
    }

    val config = scala.io.Source.fromFile(configPath).mkString
    val json = Json.parse(config)
    val active = (json \ "active").as[String]
    val clusters = (json \ "clusters").as[List[ClusterConfig]]

    _cluster = GeodesicConfig(active, clusters).clusters
      .find(_.name == active)
      .getOrElse(ClusterConfig(None, host, active))
    _cluster
  }

  def host(serviceName: String): String = {
    if (_cluster == null) {
      _cluster = load()
    }

    return _cluster.host + "/" + serviceName + "/api/v1/"
  }

  def url(serviceName: String, path: String): String = {
    if (_cluster == null) {
      _cluster = load()
    }

    return host(serviceName) + path
  }

  def getAccessToken(): Tuple2[String, String] = {
    if (!_accessToken.isEmpty) {
      return (_accessToken, _idToken)
    }

    if (_cluster == null) {
      _cluster = load()
    }

    val apiKey = _cluster.api_key.getOrElse("")
    if (apiKey.isEmpty) {
      throw new Exception("No API key found")
    }

    val krampusHost = url("krampus", "auth/token")
    val backend = createBackendWithCompression()

    var req = basicRequest
      .header("Api-Key", apiKey)
      .header("Accept-Encoding", "gzip, deflate, br")
      .get(uri"$krampusHost")
    try {
      var response = req.send(backend)
      val tokens = response.body match {
        case Right(body) => Json.parse(body).as[Tokens]
        case Left(error) =>
          throw new Exception("Error getting tokens: " + error)
      }

      _accessToken = tokens.access_token
      _idToken = tokens.id_token
    } finally backend.close()

    (_accessToken, _idToken)
  }

  def get(
      serviceName: String,
      path: String,
      query: Map[String, String]
  ): String = {
    val (accessToken, idToken) = getAccessToken()

    val queryStr = query
      .map { case (k, v) => k + "=" + v }
      .mkString("&")

    val u = uri"${url(serviceName, path)}?$queryStr"
    getURL(u.toString())
  }

  def getURL(u: String): String = {
    val (accessToken, idToken) = getAccessToken()
    val backend = createBackendWithCompression()

    var req = basicRequest
      .header("X-Auth-Request-Access-Token", "Bearer " + accessToken)
      .header("Authorization", "Bearer " + idToken)
      .header("Accept-Encoding", "gzip, deflate, br")
      .get(uri"${u}")
    try {
      var response = req.send(backend)
      response.body match {
        case Right(body) => body
        case Left(error) =>
          throw new Exception("Error getting data: " + error)
      }
    } finally backend.close()
  }

  def post(
      serviceName: String,
      path: String,
      body: JsValue
  ): String = {
    val (accessToken, idToken) = getAccessToken()
    val backend = createBackendWithCompression()

    val u = url(serviceName, path)
    var req = basicRequest
      .header("X-Auth-Request-Access-Token", "Bearer " + accessToken)
      .header("Authorization", "Bearer " + idToken)
      .header("Content-Type", "application/json")
      .header("Accept-Encoding", "gzip, deflate, br")
      .body(Json.stringify(body))
      .post(uri"${u}")
    try {
      var response = req.send(backend)
      response.body match {
        case Right(responseBody) => responseBody
        case Left(error) =>
          throw new Exception("Error posting data: " + error)
      }
    } finally backend.close()
  }

  def datasetInfo(name: String, project: String): DatasetInfo = {
    val infoStr: String =
      get(
        "boson",
        s"datasets/$project/$name/dataset-info",
        Map()
      )

    Json.parse(infoStr).as[DatasetInfo]
  }

  def search(
      name: String,
      project: String,
      pageSize: Int = 10000,
      nextLink: Option[String] = None,
      cql2Filter: Option[JsValue] = None,
      intersects: Option[JsValue] = None
  ): FeatureCollection = {
    nextLink match {
      case Some(link) =>
        val resStr = getURL(link)
        if (resStr.isEmpty) {
          throw new Exception("Error getting data: " + resStr)
        }
        return Json.parse(resStr).as[FeatureCollection]
      case None =>
    }

    val hasFilters = cql2Filter.isDefined || intersects.isDefined

    if (hasFilters) {
      logInfo(
        s"Searching dataset: $name in project: $project with pageSize: $pageSize and filters"
      )

      // Build POST request body
      var requestBody = Json.obj("limit" -> pageSize)

      cql2Filter.foreach { filter =>
        requestBody = requestBody + ("filter" -> filter)
      }

      intersects.foreach { geom =>
        requestBody = requestBody + ("intersects" -> geom)
      }

      val resStr = post(
        "boson",
        s"datasets/$project/$name/stac/search",
        requestBody
      )

      Json.parse(resStr).as[FeatureCollection]
    } else {
      logInfo(
        s"Searching dataset: $name in project: $project with pageSize: $pageSize"
      )

      val resStr: String = get(
        "boson",
        s"datasets/$project/$name/stac/search",
        Map[String, String](
          "limit" -> pageSize.toString()
        )
      )

      Json.parse(resStr).as[FeatureCollection]
    }
  }

}
