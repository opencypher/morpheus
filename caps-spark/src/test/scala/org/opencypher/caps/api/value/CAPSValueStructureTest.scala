/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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

class CAPSValueStructureTest extends CAPSValueTestSuite {

  import CAPSTestValues._

  test("Construct PATH values") {
    val originalValueGroups = PATH_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case elements: Seq[_] =>
           CAPSPath(elements.asInstanceOf[Seq[CAPSEntityValue]])

        case null =>
          cypherNull[CAPSPath]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct PATH values") {
    val cypherValueGroups = PATH_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CAPSPath(elements) => elements } }

    actual should equal(expected)

    CAPSPath.unapply(cypherNull[CAPSPath]) should equal(None)
  }

  test("Construct RELATIONSHIP values") {
    val originalValueGroups = RELATIONSHIP_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case contents: RelationshipContents =>
          val id = contents.id
          val data = contents.data
          CAPSRelationship(id, data.startId, data.endId, data.relationshipType, data.properties)

        case null =>
          cypherNull[CAPSRelationship]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct RELATIONSHIP values") {
    val cypherValueGroups = RELATIONSHIP_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { r: CAPSRelationship => RelationshipContents(r.id, r.startId, r.endId, r.relationshipType, r.properties) } }

    actual should equal(expected)

    CAPSRelationship.unapply(cypherNull[CAPSRelationship]) should equal(None)
  }

  test("Construct NODE values") {
    val originalValueGroups = NODE_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case contents: NodeContents =>
          val id = contents.id
          val data = contents.data
          CAPSNode(id, data.labels, data.properties)

        case null =>
          cypherNull[CAPSNode]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct NODE values") {
    val cypherValueGroups = NODE_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { n: CAPSNode => NodeContents(n.id, n.labels, n.properties) } }

    actual should equal(expected)

    CAPSNode.unapply(cypherNull[CAPSNode]) should equal(None)
  }

  test("Construct MAP values") {
    val originalValueGroups = MAP_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case contents: MapContents =>
          CAPSMap(contents.properties)

        case null =>
          cypherNull[CAPSMap]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct MAP values") {
    val cypherValueGroups = MAP_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CAPSMap(m) => m } }

    actual should equal(expected)

    CAPSMap.unapply(cypherNull[CAPSMap]) should equal(None)
  }

  test("Construct LIST values") {
    val originalValueGroups = LIST_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case l: Seq[_] =>
          CAPSList(l.asInstanceOf[Seq[CAPSValue]])

        case null =>
          cypherNull[CAPSList]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct LIST values") {
    val cypherValueGroups = LIST_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CAPSList(v) => v } }

    actual should equal(expected)

    CAPSList.unapply(cypherNull[CAPSList]) should equal(None)
  }

  test("Construct STRING values") {
    val originalValueGroups = STRING_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case s: String =>
          CAPSString(s)

        case null =>
          cypherNull[CAPSString]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct STRING values") {
    val cypherValueGroups = STRING_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CAPSString(v) => v } }

    actual should equal(expected)

    CAPSString.unapply(cypherNull[CAPSString]) should equal(None)
  }

  test("Construct BOOLEAN values") {
    val originalValueGroups = BOOLEAN_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case b: Boolean =>
          CAPSBoolean(b)

        case null =>
          cypherNull[CAPSBoolean]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct BOOLEAN values") {
    val cypherValueGroups = BOOLEAN_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CAPSBoolean(v) => v } }

    actual should equal(expected)

    CAPSBoolean.unapply(cypherNull[CAPSBoolean]) should equal(None)
  }

  test("Construct INTEGER values") {
    val originalValueGroups = INTEGER_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case l: Long =>
          CAPSInteger(l)

        case null =>
          cypherNull[CAPSInteger]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct INTEGER values") {
    val cypherValueGroups = INTEGER_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CAPSInteger(v) => v } }

    actual should equal(expected)

    CAPSInteger.unapply(cypherNull[CAPSInteger]) should equal(None)
  }

  test("Construct FLOAT values") {
    val originalValueGroups = FLOAT_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case d: Double =>
          CAPSFloat(d)

        case null =>
          cypherNull[CAPSFloat]

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
    val actual = cypherValueGroups.map { values => values.map { case CAPSFloat(v) => v } }

    withoutNaNs(actual) should equal(withoutNaNs(expected))

    CAPSFloat.unapply(cypherNull[CAPSFloat]) should equal(None)
  }

  test("Construct NUMBER values") {
    val originalValueGroups = NUMBER_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case l: Long =>
          CAPSInteger(l)

        case d: Double =>
          CAPSFloat(d)

        case null =>
          cypherNull[CAPSNumber]

        case _ =>
          fail("Unexpected scala value")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct NUMBER values") {
    val cypherValueGroups = NUMBER_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CAPSNumber(v) => v } }

    withoutNaNs(actual) should equal(withoutNaNs(expected))

    CAPSNumber.unapply(cypherNull[CAPSNumber]) should equal(None)
  }

  test("Construct ANY values") {
    val originalValueGroups = ANY_valueGroups
    val scalaValueGroups = originalValueGroups.scalaValueGroups

    val reconstructedValueGroups = scalaValueGroups.map {
      values => values.map {
        case contents: NodeContents =>
          val id = contents.id
          val data = contents.data
          CAPSNode(id, data.labels, data.properties)

        case contents: RelationshipContents =>
          val id = contents.id
          val data = contents.data
          CAPSRelationship(id, data)

        case contents: MapContents =>
          CAPSMap(contents.properties)

        case elements: Seq[_] if isPathLike(elements) =>
          CAPSPath(elements.asInstanceOf[Seq[CAPSEntityValue]])

        case l: Seq[_] =>
          CAPSList(l.asInstanceOf[Seq[CAPSValue]])

        case b: Boolean =>
          CAPSBoolean(b)

        case s: String =>
          CAPSString(s)

        case l: Long =>
          CAPSInteger(l)

        case d: Double =>
          CAPSFloat(d)

        case null =>
          cypherNull[CAPSValue]

        case x =>
          fail(s"Unexpected scala value $x")
      }
    }

    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct ANY values") {
    val cypherValueGroups = ANY_valueGroups.materialValueGroups

    val expected = cypherValueGroups.scalaValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CAPSValue(v) => v } }

    withoutNaNs(actual) should equal(withoutNaNs(expected))

    CAPSValue.unapply(cypherNull[CAPSValue]) should equal(None)
  }

  test("Compares nulls and material values without throwing a NPE") {
    (cypherNull[CAPSInteger] == CAPSInteger(2)) should be(false)
    (cypherNull[CAPSFloat] == cypherNull[CAPSFloat]) should be(true)
    (CAPSFloat(2.5) == cypherNull[CAPSFloat]) should be(false)
  }
}
