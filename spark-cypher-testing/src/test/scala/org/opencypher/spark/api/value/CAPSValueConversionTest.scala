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
package org.opencypher.spark.api.value

import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue._

class CAPSValueConversionTest extends CAPSValueTestSuite {

  import org.opencypher.spark.testing.api.value.CAPSTestValues._

  test("RELATIONSHIP conversion") {
    val originalValues = RELATIONSHIP_valueGroups.flatten
    val scalaValues = originalValues.map(_.unwrap)
    val newValues = scalaValues.map(scalaClass => CypherValue(scalaClass))
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isNull == (v == CypherNull))
    }
  }

  test("NODE conversion") {
    val originalValues = NODE_valueGroups.flatten
    val scalaValues = originalValues.map(_.unwrap)
    val newValues = scalaValues.map(CypherValue(_))
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isNull == (v == CypherNull))
    }
  }

  test("MAP conversion") {
    val originalValues = MAP_valueGroups.flatten
    val scalaValues = originalValues.map(_.unwrap)
    val newValues = scalaValues.map(CypherValue(_))
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isOrContainsNull == (v == CypherNull || v.as[CypherMap].get.unwrap.valuesIterator.contains(null)))
    }
  }

  test("LIST conversion") {
    val originalValues = LIST_valueGroups.flatten
    val scalaValues = originalValues.map(_.unwrap)
    val newValues = scalaValues.map {
      case null => CypherNull
      case l: List[_] => CypherList(l: _*)
    }
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isOrContainsNull == v.isNull || v.as[CypherList].get.unwrap.contains(null))
    }
  }

  test("STRING conversion") {
    val originalValues = STRING_valueGroups.flatten
    val scalaValues = originalValues.map(_.unwrap)
    val newValues = scalaValues.map {
      case null => CypherNull
      case s: java.lang.String => CypherString(s)
    }
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isOrContainsNull == (v.value == null))
    }
  }

  test("BOOLEAN conversion") {
    val originalValues = BOOLEAN_valueGroups.flatten
    val scalaValues = originalValues.map(_.value)
    val newValues = scalaValues.map {
      case null => CypherNull
      case b: Boolean => CypherBoolean(b)
    }
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isOrContainsNull == (v.value == null))
    }
  }

  test("INTEGER conversion") {
    val originalValues = INTEGER_valueGroups.flatten
    val scalaValues = originalValues.map(_.value)
    val newValues = scalaValues.map {
      case null => CypherNull
      case l: java.lang.Long => CypherInteger(l)
    }
    assert(newValues == originalValues)
    originalValues.foreach { v =>
      assert(v.isOrContainsNull == (v.value == null))
    }
  }

  test("FLOAT conversion") {
    val originalValues = FLOAT_valueGroups.flatten
    val scalaValues = originalValues.map(_.value)
    val newValues = scalaValues.map {
      case null => CypherNull
      case d: java.lang.Double => CypherFloat(d)
    }
    assert(newValues.withoutNaNs == originalValues.withoutNaNs)
    originalValues.foreach { v =>
      assert(v.isOrContainsNull == (v.value == null))
    }
  }

  test("NUMBER conversion") {
    val originalValues = NUMBER_valueGroups.flatten
    val scalaValues = originalValues.map(_.value)
    val newValues = scalaValues.map {
      case null => CypherNull
      case l: java.lang.Long => CypherInteger(l)
      case d: java.lang.Double => CypherFloat(d)
    }
    assert(newValues.withoutNaNs == originalValues.withoutNaNs)
    originalValues.foreach { v =>
      assert(v.isOrContainsNull == (v.value == null))
    }
  }

  test("ALL conversion") {
    val originalValues = ANY_valueGroups.flatten
    val scalaValues = originalValues.map(_.unwrap)
    val newValues = scalaValues.map(CypherValue(_))
    assert(newValues.withoutNaNs == originalValues.withoutNaNs)
    originalValues.foreach { v =>
      if (v == null) assert(v.isOrContainsNull)
    }
  }
}
