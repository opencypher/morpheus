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
package org.opencypher.Cypher.api.value

import org.opencypher.caps.api.value.CAPSTestValues._
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.api.value.{CypherValue, _}

class CAPSValueStructureTest extends CAPSValueTestSuite {

  //  test("Construct PATH values") {
  //    val originalValueGroups = PATH_valueGroups
  //    val scalaValueGroups = originalValueGroups.scalaValueGroups
  //
  //    val reconstructedValueGroups = scalaValueGroups.map {
  //      values => values.map {
  //        case elements: Seq[_] =>
  //           CypherPath(elements.asInstanceOf[Seq[CypherEntityValue]])
  //
  //        case null =>
  //          cypherNull[CypherPath]
  //
  //        case _ =>
  //          fail("Unexpected value")
  //      }
  //    }
  //
  //    reconstructedValueGroups should equal(originalValueGroups)
  //  }
  //
  //  test("Deconstruct PATH values") {
  //    val cypherValueGroups = PATH_valueGroups.materialValueGroups
  //
  //    val expected = cypherValueGroups.scalaValueGroups
  //    val actual = cypherValueGroups.map { values => values.map { case CypherPath(elements) => elements } }
  //
  //    actual should equal(expected)
  //
  //    CypherPath.unapply(cypherNull[CypherPath]) should equal(None)
  //  }

  test("Construct RELATIONSHIP values") {
    val originalValueGroups = RELATIONSHIP_valueGroups
    val raw = originalValueGroups.map(_.map(_.unwrap))
    val reconstructedValueGroups = raw.map(_.map(CypherValue(_)))
    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct RELATIONSHIP values") {
    val originalValueGroups = RELATIONSHIP_valueGroups
    val reconstructedValueGroups = originalValueGroups.map { values =>
      values.map {
        case CypherNull => CypherNull
        case CAPSRelationship(id, source, target, relType, properties) =>
          CAPSRelationship(id, source, target, relType, properties)
        case other => fail(s"Unexpected value $other")
      }
    }
    // ScalaTest is being silly, should equal fails, hashCode matches as well.
    assert(reconstructedValueGroups == originalValueGroups)
    CypherRelationship.unapply(null) should equal(None)
  }

  test("Construct NODE values") {
    val originalValueGroups = NODE_valueGroups
    val raw = originalValueGroups.map(_.map(_.unwrap))
    val reconstructedValueGroups = raw.map(_.map(CypherValue(_)))
    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct NODE values") {
    val originalValueGroups = NODE_valueGroups
    val reconstructedValueGroups = originalValueGroups.map { values =>
      values.map {
        case CypherNull => CypherNull
        case CAPSNode(id, labels, properties) => CAPSNode(id, labels, properties)
        case other => fail(s"Unexpected value $other")
      }
    }
    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Construct MAP values") {
    val originalValueGroups = MAP_valueGroups
    val reconstructedValueGroups = originalValueGroups.map { values =>
      values.map {
        case CypherNull => CypherNull
        case CypherMap(m) => CypherValue(m)
        case other => fail(s"Unexpected value $other")
      }
    }
    assert(reconstructedValueGroups == originalValueGroups)
  }

  test("Deconstruct MAP values") {
    val cypherValueGroups = MAP_valueGroups.materialValueGroups
    val actual = cypherValueGroups.map { values =>
      values.map {
        case CypherMap(m) => CypherMap(m)
        case other => fail(s"Unexpected value $other")
      }
    }
    assert(actual == cypherValueGroups)
  }

  test("Construct LIST values") {
    val originalValueGroups = LIST_valueGroups

    val reconstructedValueGroups = originalValueGroups.map { values =>
      values.map {
        case CypherList(l) => CypherList(l)
        case CypherNull => CypherNull
        case other => fail(s"Unexpected value $other")
      }
    }
    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct LIST values") {
    val cypherValueGroups = LIST_valueGroups.materialValueGroups
    val actual = cypherValueGroups.map { values =>
      values.map {
        case CypherList(v) => CypherList(v)
        case other => fail(s"Unexpected value $other")
      }
    }
    actual should equal(cypherValueGroups)
  }

  test("Construct STRING values") {
    val originalValueGroups = STRING_valueGroups
    val reconstructedValueGroups = originalValueGroups.map { values =>
      values.map {
        case CypherString(s) => CypherString(s)
        case CypherNull => CypherNull
        case other => fail(s"Unexpected value $other")
      }
    }
    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct STRING values") {
    val cypherValueGroups = STRING_valueGroups.materialValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherString(v) => CypherString(v) } }
    actual should equal(cypherValueGroups)
    CypherString.unapply(null.asInstanceOf[CypherString]) should equal(None)
  }

  test("Construct BOOLEAN values") {
    val originalValueGroups = BOOLEAN_valueGroups
    val reconstructedValueGroups = originalValueGroups.map { values =>
      values.map {
        case CypherBoolean(b) => CypherBoolean(b)
        case CypherNull => CypherNull
        case other => fail(s"Unexpected value $other")
      }
    }
    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct BOOLEAN values") {
    val cypherValueGroups = BOOLEAN_valueGroups.materialValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherBoolean(v) => CypherBoolean(v) } }
    actual should equal(cypherValueGroups)
  }

  test("Construct INTEGER values") {
    val originalValueGroups = INTEGER_valueGroups
    val reconstructedValueGroups = originalValueGroups.map { values =>
      values.map {
        case CypherInteger(l) => CypherInteger(l)
        case CypherNull => CypherNull
        case other => fail(s"Unexpected value $other")
      }
    }
    reconstructedValueGroups should equal(originalValueGroups)
  }

  test("Deconstruct INTEGER values") {
    val cypherValueGroups = INTEGER_valueGroups.materialValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherInteger(v) => CypherInteger(v) } }
    actual should equal(cypherValueGroups)
  }

  test("Construct FLOAT values") {
    val originalValueGroups = FLOAT_valueGroups
    val reconstructedValueGroups = originalValueGroups.map { values =>
      values.map {
        case CypherFloat(d) => CypherFloat(d)
        case CypherNull => CypherNull
        case other => fail(s"Unexpected value $other")
      }
    }
    assert(withoutNaNs(reconstructedValueGroups) == withoutNaNs(originalValueGroups))
  }

  def withoutNaNs(values: Seq[Seq[Any]]): Seq[Seq[Any]] = values.map(_.filter {
    case CypherFloat(d) => !d.isNaN
    case _ => true
  })

  test("Deconstruct FLOAT values") {
    val cypherValueGroups = FLOAT_valueGroups.materialValueGroups
    val actual = cypherValueGroups.map { values => values.map { case CypherFloat(v) => CypherFloat(v) } }
    assert(withoutNaNs(actual) == withoutNaNs(cypherValueGroups))
  }

  test("Construct NUMBER values") {
    val originalValueGroups = NUMBER_valueGroups
    val reconstructedValueGroups = originalValueGroups.map {
      values =>
        values.map {
          case CypherNull => CypherNull
          case CypherInteger(l) => CypherInteger(l)
          case CypherFloat(d) => CypherFloat(d)
          case other => fail(s"Unexpected value $other")
        }
    }
    assert(withoutNaNs(reconstructedValueGroups) == withoutNaNs(originalValueGroups))
  }

  test("Construct ANY values") {
    val originalValueGroups = ANY_valueGroups
    val reconstructedValueGroups = originalValueGroups.map { values =>
      values.map {
        case CAPSNode(id, labels, properties) =>
          CypherValue(CAPSNode(id, labels, properties))
        case CAPSRelationship(id, source, target, relType, properties) =>
          CypherValue(CAPSRelationship(id, source, target, relType, properties))
        case CypherMap(map) => CypherValue(map)
        //case elements: Seq[_] if isPathLike(elements) => CypherPath(elements.asInstanceOf[Seq[CypherEntityValue]])
        case CypherList(l) => CypherValue(l)
        case CypherBoolean(b) => CypherValue(b)
        case CypherString(s) => CypherString(s)
        case CypherInteger(l) => CypherValue(l)
        case CypherFloat(d) => CypherValue(d)
        case CypherNull => CypherValue(null)
        case other => fail(s"Unexpected value $other")
      }
    }
    assert(withoutNaNs(reconstructedValueGroups) == withoutNaNs(originalValueGroups))
  }

  test("Deconstruct ANY values") {
    val cypherValueGroups = ANY_valueGroups.materialValueGroups
    val actual = cypherValueGroups.map { values =>
      values.map {
        case CypherValue(v) => v
        case other => fail(s"Unexpected value $other")
      }
    }
    assert(withoutNaNs(actual) == withoutNaNs(cypherValueGroups))
    CypherValue.unapply(CypherNull) should equal(None)
  }

  test("Compares nulls and material values without throwing a NPE") {
    (CypherNull == (CypherInteger(2): CypherValue)) should be(false)
    (CypherNull == (CypherString(null): CypherValue)) should be(true)
    ((CypherFloat(2.5): CypherValue) == CypherNull) should be(false)
  }
}
