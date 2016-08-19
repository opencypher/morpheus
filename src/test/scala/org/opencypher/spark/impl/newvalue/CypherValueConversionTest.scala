package org.opencypher.spark.impl.newvalue

class CypherValueConversionTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("INTEGER conversion") {
    val originalValues = INTEGER_valueGroups.flatten
    val scalaValues = originalValues.map(CypherInteger.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case l: java.lang.Long => CypherInteger(l)
      case null              => null
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherInteger.containsNull(v) should equal (v == null)
    }
  }

  test("FLOAT conversion") {
    val originalValues = FLOAT_valueGroups.flatten
    val scalaValues = originalValues.map(CypherFloat.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case d: java.lang.Double => CypherFloat(d)
      case null                => null
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherFloat.containsNull(v) should equal (v == null)
    }
  }

  test("NUMBER conversion") {
    val originalValues = NUMBER_valueGroups.flatten
    val scalaValues = originalValues.map(CypherNumber.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case l: java.lang.Long   => CypherInteger(l)
      case d: java.lang.Double => CypherFloat(d)
      case null                => null
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherNumber.containsNull(v) should equal (v == null)
    }
  }
}
