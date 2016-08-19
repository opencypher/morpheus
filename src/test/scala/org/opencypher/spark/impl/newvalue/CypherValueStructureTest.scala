package org.opencypher.spark.impl.newvalue

class CypherValueStructureTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("Construct BOOLEAN values") {
    val originalValueGroups = BOOLEAN_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        opt =>
          opt match {
            case Some(b: Boolean) =>
              CypherBoolean(b)

            case None =>
              cypherNull[CypherBoolean]

            case _ =>
              fail("Unexpected scala value")
          }
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct BOOLEAN values") {
    val cypherValueGroups = BOOLEAN_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherBoolean(v) => v } }

    actual should equal(expected)

    CypherBoolean.unapply(cypherNull[CypherBoolean]) should equal(None)
  }

  test("Construct INTEGER values") {
    val originalValueGroups = INTEGER_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        opt =>
          opt match {
            case Some(l: Long) =>
              CypherInteger(l)

            case None =>
              cypherNull[CypherInteger]

            case _ =>
              fail("Unexpected scala value")
          }
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct INTEGER values") {
    val cypherValueGroups = INTEGER_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherInteger(v) => v } }

    actual should equal(expected)

    CypherInteger.unapply(cypherNull[CypherInteger]) should equal(None)
  }

  test("Construct FLOAT values") {
    val originalValueGroups = FLOAT_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        opt =>
          opt match {
            case Some(d: Double) =>
              CypherFloat(d)

            case None =>
              cypherNull[CypherFloat]

            case _ =>
              fail("Unexpected scala value")
          }
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct FLOAT values") {
    val cypherValueGroups = FLOAT_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherFloat(v) => v } }

    actual should equal(expected)

    CypherFloat.unapply(cypherNull[CypherFloat]) should equal(None)
  }

  test("Construct NUMBER values") {
    val originalValueGroups = NUMBER_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        opt =>
          opt match {
            case Some(l: Long) =>
              CypherInteger(l)

            case Some(d: Double) =>
              CypherFloat(d)

            case None =>
              cypherNull[CypherNumber]

            case _ =>
              fail("Unexpected scala value")
          }
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct NUMBER values") {
    val cypherValueGroups = NUMBER_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherNumber(v) => v } }

    actual should equal(expected)

    CypherNumber.unapply(cypherNull[CypherNumber]) should equal(None)
  }

  test("Compares nulls and material values without throwing a NPE") {
    (cypherNull[CypherInteger] == CypherInteger(2)) should be(false)
    (cypherNull[CypherFloat] == cypherNull[CypherFloat]) should be(true)
    (CypherFloat(2.5) == cypherNull[CypherFloat]) should be(false)
  }
}
