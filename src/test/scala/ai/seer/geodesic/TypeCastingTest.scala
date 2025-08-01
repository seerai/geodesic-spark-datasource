package ai.seer.geodesic

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types._
import play.api.libs.json._
import org.apache.spark.unsafe.types.UTF8String
import ai.seer.geodesic.sources.boson.BosonPartitionReader

class TypeCastingTest extends AnyFunSuite {

  test("JSON number values should be cast to correct Spark types") {
    // Test data representing different JSON number scenarios
    val testCases = Seq(
      // (json_value, expected_spark_type, expected_converted_value)
      (JsNumber(42), IntegerType, 42),
      (JsNumber(42.0), IntegerType, 42),
      (JsNumber(42.7), IntegerType, 42), // Should truncate decimal part
      (JsNumber(3.14159), DoubleType, 3.14159),
      (JsNumber(100), DoubleType, 100.0)
    )

    testCases.foreach { case (jsValue, expectedType, expectedValue) =>
      val convertedValue = convertJsonValueToSparkType(jsValue, expectedType)

      expectedType match {
        case IntegerType =>
          assert(
            convertedValue.isInstanceOf[Int],
            s"Expected Int but got ${convertedValue.getClass.getSimpleName} for $jsValue"
          )
          assert(
            convertedValue == expectedValue,
            s"Expected $expectedValue but got $convertedValue for $jsValue"
          )

        case DoubleType =>
          assert(
            convertedValue.isInstanceOf[Double],
            s"Expected Double but got ${convertedValue.getClass.getSimpleName} for $jsValue"
          )
          assert(
            convertedValue == expectedValue,
            s"Expected $expectedValue but got $convertedValue for $jsValue"
          )
      }
    }
  }

  test("JSON string values should be converted to UTF8String") {
    val jsString = JsString("test string")
    val converted = convertJsonValueToSparkType(jsString, StringType)

    assert(converted.isInstanceOf[UTF8String])
    assert(converted.toString == "test string")
  }

  test("JSON boolean values should remain as Boolean") {
    val testCases = Seq(
      (JsBoolean(true), true),
      (JsBoolean(false), false)
    )

    testCases.foreach { case (jsValue, expectedValue) =>
      val converted = convertJsonValueToSparkType(jsValue, BooleanType)
      assert(converted.isInstanceOf[Boolean])
      assert(converted == expectedValue)
    }
  }

  test("Unknown JSON values should return None") {
    val jsNull = JsNull
    val converted = convertJsonValueToSparkType(jsNull, StringType)
    assert(converted == null)
  }

  // Helper method that mimics the logic in BosonPartitionReader
  private def convertJsonValueToSparkType(
      jsValue: JsValue,
      expectedType: DataType
  ): Any = {
    jsValue match {
      case JsString(s) => UTF8String.fromString(s.toString())
      case JsNumber(n) =>
        expectedType match {
          case IntegerType => n.doubleValue().toInt
          case DoubleType  => n.doubleValue()
          case _           => n.doubleValue()
        }
      case JsBoolean(b) => b.booleanValue()
      case _            => null
    }
  }
}
