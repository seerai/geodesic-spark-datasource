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
import sttp.model.StatusCode
import sttp.model.Header

case class Tokens(access_token: String, id_token: String)

/** Geodesic error response structure */
case class GeodesicErrorDetail(
    detail: Option[String],
    `type`: Option[String],
    title: Option[String],
    status: Option[Int],
    instance: Option[String]
)

case class GeodesicError(error: Option[GeodesicErrorDetail])

object GeodesicErrorDetail {
  implicit val geodesicErrorDetailReads = Json.reads[GeodesicErrorDetail]
}

object GeodesicError {
  implicit val geodesicErrorReads = Json.reads[GeodesicError]
}

/** Custom exception for retryable HTTP status codes */
case class RetryableHttpException(
    statusCode: Int,
    message: String,
    retryAfter: Option[Int] = None
) extends Exception(s"HTTP $statusCode: $message")

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
    sys.env.getOrElse("GEODESIC_CONNECTION_TIMEOUT", "60").toInt
  private val readTimeout =
    sys.env.getOrElse("GEODESIC_READ_TIMEOUT", "60").toInt
  private val requestTimeout =
    sys.env.getOrElse("GEODESIC_REQUEST_TIMEOUT", "60").toInt

  // Retry configurations
  private val maxRetries = sys.env.getOrElse("GEODESIC_MAX_RETRIES", "3").toInt
  private val baseDelayMs =
    sys.env.getOrElse("GEODESIC_BASE_DELAY_MS", "2000").toInt

  // JWT expiration safety margin (in seconds)
  private val expirationMarginSeconds =
    sys.env.getOrElse("GEODESIC_TOKEN_EXPIRATION_MARGIN", "300").toInt

  // Retryable HTTP status codes (default: 429, 502, 503, 504)
  private val retryableHttpCodes: Set[Int] =
    sys.env
      .getOrElse("GEODESIC_RETRYABLE_HTTP_CODES", "429,500,502,503,504")
      .split(",")
      .map(_.trim.toInt)
      .toSet

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

  /** Check if an exception is retryable based on its type and message
    */
  private def isRetryableException(e: Exception): Boolean = e match {
    case _: RetryableHttpException => true
    case ex: Exception =>
      val msg = ex.getMessage
      msg != null && (
        msg.contains("timeout") ||
          msg.contains("connection") ||
          msg.contains("ConnectException") ||
          msg.contains("SocketTimeoutException") ||
          msg.contains("HttpTimeoutException") ||
          msg.contains("ConnectTimeoutException") ||
          msg.contains("ReadTimeoutException")
      )
    case _ => false
  }

  /** Calculate retry delay based on exception type
    */
  private def calculateRetryDelay(e: Exception, attempt: Int): Int = e match {
    case httpEx: RetryableHttpException =>
      httpEx.retryAfter match {
        case Some(retryAfterSeconds) =>
          logWarning(
            s"Received HTTP ${httpEx.statusCode}, retrying after ${retryAfterSeconds} seconds as specified by Retry-After header"
          )
          retryAfterSeconds * 1000
        case None =>
          val delay =
            baseDelayMs * Math.pow(2, attempt).toInt + Random.nextInt(1000)
          logWarning(
            s"Received HTTP ${httpEx.statusCode}, retrying in ${delay}ms"
          )
          delay
      }
    case _ =>
      val delay =
        baseDelayMs * Math.pow(2, attempt).toInt + Random.nextInt(1000)
      logWarning(s"Request failed, retrying in ${delay}ms: ${e.getMessage}")
      delay
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

          // Check if we should retry
          if (!isRetryableException(e) || attempt == maxRetries - 1) {
            val errorMsg = e match {
              case httpEx: RetryableHttpException =>
                s"$operationName failed on attempt ${attempt + 1} with HTTP ${httpEx.statusCode}: ${httpEx.getMessage}"
              case _ =>
                s"$operationName failed on attempt ${attempt + 1}: ${e.getMessage}"
            }
            logError(errorMsg)
            throw e
          }

          // Calculate and apply retry delay
          val delayMs = calculateRetryDelay(e, attempt)

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

  /** Parse Geodesic error response and extract meaningful error message
    */
  private def parseGeodesicError(errorBody: String): String = {
    Try {
      val json = Json.parse(errorBody)
      val geodesicError = json.as[GeodesicError]

      geodesicError.error match {
        case Some(errorDetail) =>
          val parts = List(
            errorDetail.title,
            errorDetail.detail,
            errorDetail.`type`,
            errorDetail.instance
          ).flatten

          if (parts.nonEmpty) {
            parts.mkString(" - ")
          } else {
            errorBody
          }
        case None => errorBody
      }
    }.getOrElse(errorBody)
  }

  /** Centralized HTTP request method with retry logic and proper error handling
    */
  private def executeHttpRequest(
      requestBuilder: () => sttp.client3.RequestT[sttp.client3.Identity, Either[
        String,
        String
      ], Any],
      operationName: String
  ): String = {
    withRetry(
      () => {
        val backend = createBackendWithCompression()
        try {
          val request = requestBuilder()
          val response = request.send(backend)

          // Check if the status code is retryable
          if (retryableHttpCodes.contains(response.code.code)) {
            // Extract Retry-After header if present (for 429 responses)
            val retryAfter = if (response.code.code == 429) {
              response.header("Retry-After").flatMap { value =>
                Try(value.toInt).toOption
              }
            } else {
              None
            }

            // Try to parse error message from response body
            val errorMessage = response.body match {
              case Left(error) => parseGeodesicError(error)
              case Right(_) =>
                s"Received retryable HTTP status code ${response.code.code}"
            }

            throw RetryableHttpException(
              response.code.code,
              errorMessage,
              retryAfter
            )
          }

          response.body match {
            case Right(body) => body.toString
            case Left(error) =>
              // Parse Geodesic error format for better error messages
              val errorMessage = parseGeodesicError(error)
              throw new Exception(
                s"HTTP request failed with status ${response.code.code}: $errorMessage"
              )
          }
        } finally {
          backend.close()
        }
      },
      operationName
    )
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

    // Use centralized HTTP request method for token requests
    val krampusHost = url("krampus", "auth/token")
    val responseBody = executeHttpRequest(
      () =>
        basicRequest
          .header("Api-Key", apiKey)
          .header("Accept-Encoding", "gzip, deflate, br")
          .readTimeout(readTimeout.seconds)
          .get(uri"$krampusHost"),
      "Token request"
    )

    val tokens = Json.parse(responseBody).as[Tokens]

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

    executeHttpRequest(
      () =>
        basicRequest
          .header("X-Auth-Request-Access-Token", "Bearer " + accessToken)
          .header("Authorization", "Bearer " + idToken)
          .header("Accept-Encoding", "gzip, deflate, br")
          .readTimeout(readTimeout.seconds)
          .get(uri"${u}"),
      s"GET request to $u"
    )
  }

  def post(
      serviceName: String,
      path: String,
      body: JsValue
  ): String = {
    val (accessToken, idToken) = getAccessToken()
    val u = url(serviceName, path)

    executeHttpRequest(
      () =>
        basicRequest
          .header("X-Auth-Request-Access-Token", "Bearer " + accessToken)
          .header("Authorization", "Bearer " + idToken)
          .header("Content-Type", "application/json")
          .header("Accept-Encoding", "gzip, deflate, br")
          .readTimeout(readTimeout.seconds)
          .body(Json.stringify(body))
          .post(uri"${u}"),
      s"POST request to $u"
    )
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

    logInfo(
      s"Searching dataset: $name, collection: $collectionId, in project: $project, with pageSize: $pageSize"
    )

    // Build POST request body
    var requestBody = Json.obj("limit" -> pageSize)
    // Add collections as an array of strings
    requestBody =
      requestBody + ("collections" -> JsArray(Seq(JsString(collectionId))))

    cql2Filter.foreach { filter =>
      requestBody = requestBody + ("filter" -> filter)
    }

    intersects.foreach { geom =>
      requestBody = requestBody + ("intersects" -> geom)
    }
    logInfo(
      s"Request body for dataset search: ${Json.prettyPrint(requestBody)}"
    )
    val resStr = post(
      "boson",
      s"datasets/$project/$name/stac/search",
      requestBody
    )

    Json.parse(resStr).as[FeatureCollection]

  }

}
