package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.api.EntityId

class CypherValueStructureTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("Construct NODE values") {
    val originalValueGroups = NODE_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case (id: EntityId, data: NodeData) =>
           CypherNode(id, data.labels, Properties.fromMap(data.properties))

        case null =>
          cypherNull[CypherNode]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct NODE values") {
    val cypherValueGroups = NODE_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherNode(id, data) => id -> data } }

    actual should equal(expected)

    CypherNode.unapply(cypherNull[CypherNode]) should equal(None)
  }


  test("Construct MAP values") {
    val originalValueGroups = MAP_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case p: Properties =>
          CypherMap(p)

        case null =>
          cypherNull[CypherMap]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct MAP values") {
    val cypherValueGroups = MAP_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherMap(m) => m } }

    actual should equal(expected)

    CypherMap.unapply(cypherNull[CypherMap]) should equal(None)
  }

  test("Construct LIST values") {
    val originalValueGroups = LIST_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case l: Seq[_] =>
          CypherList(l.asInstanceOf[Seq[CypherValue]])

        case null =>
          cypherNull[CypherList]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct LIST values") {
    val cypherValueGroups = LIST_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherList(v) => v } }

    actual should equal(expected)

    CypherList.unapply(cypherNull[CypherList]) should equal(None)
  }

  test("Construct STRING values") {
    val originalValueGroups = STRING_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case s: String =>
          CypherString(s)

        case null =>
          cypherNull[CypherString]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct STRING values") {
    val cypherValueGroups = STRING_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherString(v) => v } }

    actual should equal(expected)

    CypherString.unapply(cypherNull[CypherString]) should equal(None)
  }

  test("Construct BOOLEAN values") {
    val originalValueGroups = BOOLEAN_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case b: Boolean =>
          CypherBoolean(b)

        case null =>
          cypherNull[CypherBoolean]

        case _ =>
          fail("Unexpected scala value")
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
        case l: Long =>
          CypherInteger(l)

        case null =>
          cypherNull[CypherInteger]

        case _ =>
          fail("Unexpected scala value")
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
        case d: Double =>
          CypherFloat(d)

        case null =>
          cypherNull[CypherFloat]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  def withoutNaNs(values: Seq[Seq[Any]]) = values.map(_.filter {
    case d: Double => !d.isNaN
    case _         => true
  })

  test("Deconstruct FLOAT values") {
    val cypherValueGroups = FLOAT_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherFloat(v) => v } }

    withoutNaNs(actual) should equal(withoutNaNs(expected))

    CypherFloat.unapply(cypherNull[CypherFloat]) should equal(None)
  }

  test("Construct NUMBER values") {
    val originalValueGroups = NUMBER_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case l: Long =>
          CypherInteger(l)

        case d: Double =>
          CypherFloat(d)

        case null =>
          cypherNull[CypherNumber]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct NUMBER values") {
    val cypherValueGroups = NUMBER_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherNumber(v) => v } }

    withoutNaNs(actual) should equal(withoutNaNs(expected))

    CypherNumber.unapply(cypherNull[CypherNumber]) should equal(None)
  }

  test("Construct ANY values") {
    val originalValueGroups = ANY_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case (id: EntityId, data: NodeData) =>
          CypherNode(id, data.labels, data.properties)

        case p: Properties =>
          CypherMap(p)

        case l: Seq[_] =>
          CypherList(l.asInstanceOf[Seq[CypherValue]])

        case b: Boolean =>
          CypherBoolean(b)

        case s: String =>
          CypherString(s)

        case l: Long =>
          CypherInteger(l)

        case d: Double =>
          CypherFloat(d)

        case null =>
          cypherNull[CypherValue]

        case x =>
          fail(s"Unexpected scala value $x")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct ANY values") {
    val cypherValueGroups = ANY_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherValue(v) => v } }

    withoutNaNs(actual) should equal(withoutNaNs(expected))

    CypherValue.unapply(cypherNull[CypherValue]) should equal(None)
  }

  test("Compares nulls and material values without throwing a NPE") {
    (cypherNull[CypherInteger] == CypherInteger(2)) should be(false)
    (cypherNull[CypherFloat] == cypherNull[CypherFloat]) should be(true)
    (CypherFloat(2.5) == cypherNull[CypherFloat]) should be(false)
  }
}
