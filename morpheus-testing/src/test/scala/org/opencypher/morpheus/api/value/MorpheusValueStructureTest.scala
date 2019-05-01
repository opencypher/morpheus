/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.morpheus.api.value

import org.opencypher.morpheus.testing.api.value.MorpheusTestValues._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherFloat, _}

class MorpheusValueStructureTest extends MorpheusValueTestSuite {

  describe("RELATIONSHIP") {
    it("constructs values") {
      val originalValueGroups = RELATIONSHIP_valueGroups
      val raw = originalValueGroups.map(_.map(_.unwrap))
      val reconstructedValueGroups = raw.map(_.map(CypherValue(_)))
      reconstructedValueGroups should equal(originalValueGroups)
    }

    it("deconstructs values") {
      val originalValueGroups = RELATIONSHIP_valueGroups
      val reconstructedValueGroups = originalValueGroups.map { values =>
        values.map {
          case CypherNull => CypherNull
          case MorpheusRelationship(id, source, target, relType, properties) =>
            MorpheusRelationship(id, source, target, relType, properties)
          case other => fail(s"Unexpected value $other")
        }
      }
      // ScalaTest is being silly, should equal fails, hashCode matches as well.
      assert(reconstructedValueGroups == originalValueGroups)
      Relationship.unapply(null) should equal(None)
    }
  }

  describe("NODE") {
    it("constructs values") {
      val originalValueGroups = NODE_valueGroups
      val raw = originalValueGroups.map(_.map(_.unwrap))
      val reconstructedValueGroups = raw.map(_.map(CypherValue(_)))
      reconstructedValueGroups should equal(originalValueGroups)
    }

    it("deconstructs values") {
      val originalValueGroups = NODE_valueGroups
      val reconstructedValueGroups = originalValueGroups.map { values =>
        values.map {
          case CypherNull => CypherNull
          case MorpheusNode(id, labels, properties) => MorpheusNode(id, labels, properties)
          case other => fail(s"Unexpected value $other")
        }
      }
      reconstructedValueGroups should equal(originalValueGroups)
    }
  }

  describe("MAP") {
    it("constructs values") {
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

    it("deconstructs values") {
      val cypherValueGroups = MAP_valueGroups.materialValueGroups
      val actual = cypherValueGroups.map { values =>
        values.map {
          case CypherMap(m) => CypherMap(m)
          case other => fail(s"Unexpected value $other")
        }
      }
      assert(actual == cypherValueGroups)
    }
  }


  describe("LIST") {
    it("constructs values") {
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

    it("Deconstruct LIST values") {
      val cypherValueGroups = LIST_valueGroups.materialValueGroups
      val actual = cypherValueGroups.map { values =>
        values.map {
          case CypherList(v) => CypherList(v)
          case other => fail(s"Unexpected value $other")
        }
      }
      actual should equal(cypherValueGroups)
    }
  }

  describe("STRING") {
    it("constructs values") {
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

    it("Deconstruct STRING values") {
      val cypherValueGroups = STRING_valueGroups.materialValueGroups
      val actual = cypherValueGroups.map { values => values.map { case CypherString(v) => CypherString(v) } }
      actual should equal(cypherValueGroups)
      CypherString.unapply(null.asInstanceOf[CypherString]) should equal(None)
    }
  }

  describe("BOOLEAN") {
    it("constructs values") {
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

    it("Deconstruct BOOLEAN values") {
      val cypherValueGroups = BOOLEAN_valueGroups.materialValueGroups
      val actual = cypherValueGroups.map { values => values.map { case CypherBoolean(v) => CypherBoolean(v) } }
      actual should equal(cypherValueGroups)
    }
  }

  describe("INTEGER") {
    it("constructs values") {
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

    it("Deconstruct INTEGER values") {
      val cypherValueGroups = INTEGER_valueGroups.materialValueGroups
      val actual = cypherValueGroups.map { values => values.map { case CypherInteger(v) => CypherInteger(v) } }
      actual should equal(cypherValueGroups)
    }
  }

  describe("FLOAT") {
    it("constructs values") {
      val originalValueGroups = FLOAT_valueGroups
      val reconstructedValueGroups = originalValueGroups.map { values =>
        values.map {
          case CypherFloat(d) => CypherFloat(d)
          case CypherNull => CypherNull
          case other => fail(s"Unexpected value $other")
        }
      }
      assert(reconstructedValueGroups.withoutNaNs == originalValueGroups.withoutNaNs)
    }

    it("Deconstruct FLOAT values") {
      val cypherValueGroups = FLOAT_valueGroups.materialValueGroups
      val actual = cypherValueGroups.map { values => values.map { case CypherFloat(v) => CypherFloat(v) } }
      assert(actual.withoutNaNs == cypherValueGroups.withoutNaNs)
    }
  }

  describe("BIGDECIMAL") {
    it("constructs values") {
      val originalValueGroups = BIGDECIMAL_valueGroups
      val reconstructedValueGroups = originalValueGroups.map { values =>
        values.map {
          case CypherBigDecimal(d) => CypherBigDecimal(d)
          case CypherNull => CypherNull
          case other => fail(s"Unexpected value $other")
        }
      }
      assert(reconstructedValueGroups.withoutNaNs == originalValueGroups.withoutNaNs)
    }

    it("deconstructs values") {
      val cypherValueGroups = BIGDECIMAL_valueGroups.materialValueGroups
      val actual = cypherValueGroups.map { values => values.map { case CypherBigDecimal(v) => CypherBigDecimal(v) } }
      assert(actual.withoutNaNs == cypherValueGroups.withoutNaNs)
    }
  }

  describe("NUMBER") {
    it("constructs values") {
      val originalValueGroups = NUMBER_valueGroups
      val reconstructedValueGroups = originalValueGroups.map {
        values =>
          values.map {
            case CypherNull => CypherNull
            case CypherInteger(l) => CypherInteger(l)
            case CypherFloat(d) => CypherFloat(d)
            case CypherBigDecimal(d) => CypherBigDecimal(d)
            case other => fail(s"Unexpected value $other")
          }
      }
      assert(reconstructedValueGroups.withoutNaNs == originalValueGroups.withoutNaNs)
    }

    it("deconstructs values") {
      val originalValueGroups = ANY_valueGroups
      val reconstructedValueGroups = originalValueGroups.map { values =>
        values.map {
          case MorpheusNode(id, labels, properties) =>
            CypherValue(MorpheusNode(id, labels, properties))
          case MorpheusRelationship(id, source, target, relType, properties) =>
            CypherValue(MorpheusRelationship(id, source, target, relType, properties))
          case CypherMap(map) => CypherValue(map)
          case CypherList(l) => CypherValue(l)
          case CypherBoolean(b) => CypherValue(b)
          case CypherString(s) => CypherString(s)
          case CypherInteger(l) => CypherValue(l)
          case CypherFloat(d) => CypherValue(d)
          case CypherBigDecimal(d) => CypherValue(d)
          case CypherNull => CypherValue(null)
          case other => fail(s"Unexpected value $other")
        }
      }
      assert(reconstructedValueGroups.withoutNaNs == originalValueGroups.withoutNaNs)
    }
  }

  describe("ANY"){
    it("deconstructs values") {
      val cypherValueGroups = ANY_valueGroups.materialValueGroups
      val actual = cypherValueGroups.map { values =>
        values.map {
          case CypherValue(v) => CypherValue(v)
          case other => fail(s"Unexpected value $other")
        }
      }
      assert(actual.withoutNaNs == cypherValueGroups.withoutNaNs)
      CypherValue.unapply(CypherNull) should equal(None)
    }

    it("Compares nulls and material values without throwing a NPE") {
      (CypherNull == (CypherInteger(2): CypherValue)) should be(false)
      (CypherNull == (CypherString(null): CypherValue)) should be(true)
      ((CypherFloat(2.5): CypherValue) == CypherNull) should be(false)
    }
  }
}
