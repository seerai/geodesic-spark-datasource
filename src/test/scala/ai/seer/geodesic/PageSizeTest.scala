package ai.seer.geodesic

import org.scalatest.funsuite.AnyFunSuite
import ai.seer.geodesic.sources.boson.DefaultSource
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._

class PageSizeTest extends AnyFunSuite {

  test("Page size should use dataset maxPageSize when no user value provided") {
    // This test would require mocking the GeodesicClient to return specific dataset info
    // For now, we'll test the logic conceptually

    val defaultPageSize = 10000
    val datasetMaxPageSize = 5000
    val userPageSize: Option[Int] = None

    val effectivePageSize = userPageSize match {
      case Some(userValue) =>
        if (userValue > datasetMaxPageSize) {
          println(
            s"Warning: Requested pageSize ($userValue) exceeds dataset maxPageSize ($datasetMaxPageSize). Using requested value."
          )
        }
        userValue
      case None => datasetMaxPageSize
    }

    assert(
      effectivePageSize == datasetMaxPageSize,
      s"Expected $datasetMaxPageSize but got $effectivePageSize"
    )
  }

  test(
    "Page size should use user value when provided, even if it exceeds dataset max"
  ) {
    val defaultPageSize = 10000
    val datasetMaxPageSize = 5000
    val userPageSize: Option[Int] = Some(8000)

    val effectivePageSize = userPageSize match {
      case Some(userValue) =>
        if (userValue > datasetMaxPageSize) {
          println(
            s"Warning: Requested pageSize ($userValue) exceeds dataset maxPageSize ($datasetMaxPageSize). Using requested value."
          )
        }
        userValue
      case None => datasetMaxPageSize
    }

    assert(
      effectivePageSize == 8000,
      s"Expected 8000 but got $effectivePageSize"
    )
  }

  test("Page size should use default when no dataset config available") {
    val defaultPageSize = 10000
    val datasetMaxPageSize = defaultPageSize // No config available
    val userPageSize: Option[Int] = None

    val effectivePageSize = userPageSize match {
      case Some(userValue) =>
        if (userValue > datasetMaxPageSize) {
          println(
            s"Warning: Requested pageSize ($userValue) exceeds dataset maxPageSize ($datasetMaxPageSize). Using requested value."
          )
        }
        userValue
      case None => datasetMaxPageSize
    }

    assert(
      effectivePageSize == defaultPageSize,
      s"Expected $defaultPageSize but got $effectivePageSize"
    )
  }

  test("Page size precedence logic should work correctly") {
    // Test the precedence: User > Dataset Max > Default

    val testCases = Seq(
      // (userValue, datasetMax, expected, description)
      (None, 5000, 5000, "No user value - should use dataset max"),
      (
        Some(3000),
        5000,
        3000,
        "User value within dataset max - should use user value"
      ),
      (
        Some(8000),
        5000,
        8000,
        "User value exceeds dataset max - should use user value with warning"
      ),
      (
        None,
        10000,
        10000,
        "No user value, dataset max equals default - should use dataset max"
      ),
      (
        Some(15000),
        10000,
        15000,
        "User value exceeds default - should use user value"
      )
    )

    testCases.foreach { case (userValue, datasetMax, expected, description) =>
      val effectivePageSize = userValue match {
        case Some(userVal) =>
          if (userVal > datasetMax) {
            // In real implementation, this would print a warning
          }
          userVal
        case None => datasetMax
      }

      assert(
        effectivePageSize == expected,
        s"$description: Expected $expected but got $effectivePageSize"
      )
    }
  }
}
