/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.api.value

class CypherValueStructureTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("Construct PATH values") {
    val originalValueGroups = PATH_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
        case elements: Seq[_] =>
          CypherPath(elements.asInstanceOf[Seq[CypherEntityValue]])

        case null =>
          cypherNull[CypherPath]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct PATH values") {
    val cypherValueGroups = PATH_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values =>
      values.map { case CypherPath(elements) => elements }
    }

    actual should equal(expected)

    CypherPath.unapply(cypherNull[CypherPath]) should equal(None)
  }

  test("Construct RELATIONSHIP values") {
    val originalValueGroups = RELATIONSHIP_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
        case contents: RelationshipContents =>
          val id   = contents.id
          val data = contents.data
          CypherRelationship(id, data.startId, data.endId, data.relationshipType, data.properties)

        case null =>
          cypherNull[CypherRelationship]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct RELATIONSHIP values") {
    val cypherValueGroups = RELATIONSHIP_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values =>
      values.map { r: CypherRelationship =>
        RelationshipContents(r.id, r.startId, r.endId, r.relationshipType, r.properties)
      }
    }

    actual should equal(expected)

    CypherRelationship.unapply(cypherNull[CypherRelationship]) should equal(None)
  }

  test("Construct NODE values") {
    val originalValueGroups = NODE_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
        case contents: NodeContents =>
          val id   = contents.id
          val data = contents.data
          CypherNode(id, data.labels, data.properties)

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
    val actual = cypherValueGroups.map { values =>
      values.map { n: CypherNode =>
        NodeContents(n.id, n.labels, n.properties)
      }
    }

    actual should equal(expected)

    CypherNode.unapply(cypherNull[CypherNode]) should equal(None)
  }

  test("Construct MAP values") {
    val originalValueGroups = MAP_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
        case contents: MapContents =>
          CypherMap(contents.properties)

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
    val actual = cypherValueGroups.map { values =>
      values.map { case CypherMap(m) => m }
    }

    actual should equal(expected)

    CypherMap.unapply(cypherNull[CypherMap]) should equal(None)
  }

  test("Construct LIST values") {
    val originalValueGroups = LIST_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
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
    val actual = cypherValueGroups.map { values =>
      values.map { case CypherList(v) => v }
    }

    actual should equal(expected)

    CypherList.unapply(cypherNull[CypherList]) should equal(None)
  }

  test("Construct STRING values") {
    val originalValueGroups = STRING_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
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
    val actual = cypherValueGroups.map { values =>
      values.map { case CypherString(v) => v }
    }

    actual should equal(expected)

    CypherString.unapply(cypherNull[CypherString]) should equal(None)
  }

  test("Construct BOOLEAN values") {
    val originalValueGroups = BOOLEAN_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
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
    val actual = cypherValueGroups.map { values =>
      values.map { case CypherBoolean(v) => v }
    }

    actual should equal(expected)

    CypherBoolean.unapply(cypherNull[CypherBoolean]) should equal(None)
  }

  test("Construct INTEGER values") {
    val originalValueGroups = INTEGER_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
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
    val actual = cypherValueGroups.map { values =>
      values.map { case CypherInteger(v) => v }
    }

    actual should equal(expected)

    CypherInteger.unapply(cypherNull[CypherInteger]) should equal(None)
  }

  test("Construct FLOAT values") {
    val originalValueGroups = FLOAT_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
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

  def withoutNaNs(values: Seq[Seq[Any]]) =
    values.map(_.filter {
      case d: Double => !d.isNaN
      case _         => true
    })

  test("Deconstruct FLOAT values") {
    val cypherValueGroups = FLOAT_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values =>
      values.map { case CypherFloat(v) => v }
    }

    withoutNaNs(actual) should equal(withoutNaNs(expected))

    CypherFloat.unapply(cypherNull[CypherFloat]) should equal(None)
  }

  test("Construct NUMBER values") {
    val originalValueGroups = NUMBER_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
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
    val actual = cypherValueGroups.map { values =>
      values.map { case CypherNumber(v) => v }
    }

    withoutNaNs(actual) should equal(withoutNaNs(expected))

    CypherNumber.unapply(cypherNull[CypherNumber]) should equal(None)
  }

  test("Construct ANY values") {
    val originalValueGroups = ANY_valueGroups
    val scalaValueGroups    = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map { values =>
      values.map {
        case contents: NodeContents =>
          val id   = contents.id
          val data = contents.data
          CypherNode(id, data.labels, data.properties)

        case contents: RelationshipContents =>
          val id   = contents.id
          val data = contents.data
          CypherRelationship(id, data)

        case contents: MapContents =>
          CypherMap(contents.properties)

        case elements: Seq[_] if isPathLike(elements) =>
          CypherPath(elements.asInstanceOf[Seq[CypherEntityValue]])

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
    val actual = cypherValueGroups.map { values =>
      values.map { case CypherValue(v) => v }
    }

    withoutNaNs(actual) should equal(withoutNaNs(expected))

    CypherValue.unapply(cypherNull[CypherValue]) should equal(None)
  }

  test("Compares nulls and material values without throwing a NPE") {
    (cypherNull[CypherInteger] == CypherInteger(2)) should be(false)
    (cypherNull[CypherFloat] == cypherNull[CypherFloat]) should be(true)
    (CypherFloat(2.5) == cypherNull[CypherFloat]) should be(false)
  }
}
