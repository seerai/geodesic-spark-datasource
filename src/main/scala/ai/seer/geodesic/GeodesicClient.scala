package ai.seer.geodesic

import play.api.libs.json._
import org.apache.spark.internal.Logging
import sttp.client3.{basicRequest, UriContext, HttpClientSyncBackend}
import java.io.Serializable
import sttp.client3.{Request, Response}
import com.auth0.jwt.JWT
import com.auth0.jwt.exceptions.JWTDecodeException
import java.time.Instant
import java.util.Date
import scala.util.{Try, Success, Failure}
import scala.util.Random
import scala.concurrent.duration._
case class Tokens(access_token: String, id_token: String)

object Tokens {
  implicit val tokensReads = Json.reads[Tokens]
}

/** Thread-safe singleton for caching tokens across serialization boundaries */
object TokenCache {
  @volatile private var cachedAccessToken: String = ""
  @volatile private var cachedIdToken: String = ""
  @volatile private var lastTokenTime: Long = 0L

  def getTokens(): Option[(String, String)] = {
    if (cachedAccessToken.nonEmpty && cachedIdToken.nonEmpty) {
      Some((cachedAccessToken, cachedIdToken))
    } else {
      None
    }
  }

  def setTokens(accessToken: String, idToken: String): Unit = {
    cachedAccessToken = accessToken
    cachedIdToken = idToken
    lastTokenTime = System.currentTimeMillis()
  }

  def clearTokens(): Unit = {
    cachedAccessToken = ""
    cachedIdToken = ""
    lastTokenTime = 0L
  }

  def getAccessToken(): String = cachedAccessToken
  def getIdToken(): String = cachedIdToken
}

class GeodesicClient(accessToken: String = "", idToken: String = "")
    extends Serializable
    with Logging {
  private var _accessToken = accessToken
  private var _idToken = idToken
  private var _cluster: ClusterConfig = _

  // Timeout configurations (in seconds)
  private val connectionTimeout =
    sys.env.getOrElse("GEODESIC_CONNECTION_TIMEOUT", "10").toInt
  private val readTimeout =
    sys.env.getOrElse("GEODESIC_READ_TIMEOUT", "30").toInt
  private val requestTimeout =
    sys.env.getOrElse("GEODESIC_REQUEST_TIMEOUT", "60").toInt

  // Retry configurations
  private val maxRetries = sys.env.getOrElse("GEODESIC_MAX_RETRIES", "3").toInt
  private val baseDelayMs =
    sys.env.getOrElse("GEODESIC_BASE_DELAY_MS", "1000").toInt

  // JWT expiration safety margin (in seconds)
  private val expirationMarginSeconds =
    sys.env.getOrElse("GEODESIC_TOKEN_EXPIRATION_MARGIN", "300").toInt

  /** Create an HTTP client backend with compression and timeout configurations
    */
  private def createBackendWithCompression() = {
    import sttp.client3.HttpClientSyncBackend

    HttpClientSyncBackend(
      options = sttp.client3.SttpBackendOptions(
        connectionTimeout = connectionTimeout.seconds,
        proxy = None
      )
    )
  }

  /** Check if a JWT token is expired or will expire within the safety margin
    */
  private def isTokenExpired(token: String): Boolean = {
    if (token.isEmpty) return true

    Try {
      val decodedJWT = JWT.decode(token)
      val expiresAt = decodedJWT.getExpiresAt
      if (expiresAt == null) {
        logWarning("JWT token has no expiration claim, treating as expired")
        return true
      }

      val now = Instant.now()
      val expirationWithMargin =
        expiresAt.toInstant.minusSeconds(expirationMarginSeconds)
      val isExpired = now.isAfter(expirationWithMargin)
      if (isExpired) {
        logInfo(
          s"JWT token expired or expires within ${expirationMarginSeconds} seconds"
        )
      }

      isExpired
    } match {
      case Success(expired) => expired
      case Failure(e: JWTDecodeException) =>
        logWarning(s"Failed to decode JWT token: ${e.getMessage}")
        true
      case Failure(e) =>
        logWarning(s"Unexpected error checking JWT expiration: ${e.getMessage}")
        true
    }
  }

  /** Execute a function with retry logic and exponential backoff
    */
  private def withRetry[T](operation: () => T, operationName: String): T = {
    var lastException: Exception = null

    for (attempt <- 0 until maxRetries) {
      try {
        return operation()
      } catch {
        case e: Exception =>
          lastException = e
          val isRetryableError = e.getMessage.contains("timeout") ||
            e.getMessage.contains("connection") ||
            e.getMessage.contains("ConnectException") ||
            e.getMessage.contains("SocketTimeoutException")

          if (!isRetryableError || attempt == maxRetries - 1) {
            logError(
              s"$operationName failed on attempt ${attempt + 1}: ${e.getMessage}"
            )
            throw e
          }

          val delayMs =
            baseDelayMs * Math.pow(2, attempt).toInt + Random.nextInt(1000)
          logWarning(
            s"$operationName failed on attempt ${attempt + 1}, retrying in ${delayMs}ms: ${e.getMessage}"
          )

          try {
            Thread.sleep(delayMs)
          } catch {
            case _: InterruptedException =>
              Thread.currentThread().interrupt()
              throw e
          }
      }
    }

    throw lastException
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
    // First check constructor-provided tokens
    if (!_accessToken.isEmpty && !isTokenExpired(_accessToken)) {
      logInfo(
        s"Using constructor-provided token, expiration check: ${isTokenExpired(_accessToken)}"
      )
      return (_accessToken, _idToken)
    }

    // Then check singleton cache for tokens that survive serialization
    TokenCache.getTokens() match {
      case Some((cachedAccessToken, cachedIdToken))
          if !isTokenExpired(cachedAccessToken) =>
        return (cachedAccessToken, cachedIdToken)
      case Some((cachedAccessToken, _)) if isTokenExpired(cachedAccessToken) =>
        logInfo("Cached token expired, clearing cache")
        TokenCache.clearTokens()
      case _ =>
        logInfo("No cached token available")
    }

    // Clear expired instance tokens
    if (!_accessToken.isEmpty && isTokenExpired(_accessToken)) {
      logInfo("Clearing expired instance access token")
      _accessToken = ""
      _idToken = ""
    }

    if (_cluster == null) {
      _cluster = load()
    }

    val apiKey = _cluster.api_key.getOrElse("")
    if (apiKey.isEmpty) {
      throw new Exception("No API key found")
    }

    // Use retry logic for token requests
    val tokens = withRetry(
      () => {
        val krampusHost = url("krampus", "auth/token")
        val backend = createBackendWithCompression()

        var req = basicRequest
          .header("Api-Key", apiKey)
          .header("Accept-Encoding", "gzip, deflate, br")
          .readTimeout(readTimeout.seconds)
          .get(uri"$krampusHost")

        try {
          var response = req.send(backend)
          response.body match {
            case Right(body) => Json.parse(body.toString).as[Tokens]
            case Left(error) =>
              throw new Exception("Error getting tokens: " + error)
          }
        } finally backend.close()
      },
      "Token request"
    )

    // Cache tokens in both instance and singleton cache
    _accessToken = tokens.access_token
    _idToken = tokens.id_token
    TokenCache.setTokens(tokens.access_token, tokens.id_token)

    logInfo("Successfully obtained new access token")
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
      .readTimeout(readTimeout.seconds)
      .get(uri"${u}")
    try {
      var response = req.send(backend)
      response.body match {
        case Right(body) => body.toString
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
      .readTimeout(readTimeout.seconds)
      .body(Json.stringify(body))
      .post(uri"${u}")
    try {
      var response = req.send(backend)
      response.body match {
        case Right(responseBody) => responseBody.toString
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
      collectionId: String,
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
