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
package org.opencypher.okapi.api.value

import org.opencypher.okapi.api.value.CypherValue._

object CAPSTestValues {

  // This object provides various seqs of cypher values sorted by orderability for use in tests.
  //
  // List items are sequences of items that are equivalent.
  //
  // These inner seqs are supposed to be duplicate free
  //
  // Note: We can't use sets here as that would mean we'd use the equality in the test data that we're about to test
  //
  type ValueGroups[V] = Seq[Values[V]]
  type Values[V] = Seq[V]

  implicit lazy val RELATIONSHIP_valueGroups: ValueGroups[CypherValue] = Seq(
    Seq[CypherValue](
      CAPSRelationship(1, 1, 1, "KNOWS", CypherMap("a" -> 1, "b" -> CypherNull)),
      CAPSRelationship(1, 2, 4, "FORGETS", CypherMap("a" -> 1.0, "b" -> CypherNull))
    ),
    Seq[CypherValue](CAPSRelationship(10, 1, 1, "KNOWS", CypherMap("a" -> 1))),
    Seq[CypherValue](
      CAPSRelationship(20, 1, 1, "KNOWS", CypherMap("a" -> 1, "b" -> 1))),
    Seq[CypherValue](
      CAPSRelationship(21, 0, -1, "KNOWS", CypherMap("b" -> CypherNull))),
    Seq[CypherValue](CAPSRelationship(30, 1, 1, "_-&", CypherMap.empty)),
    Seq[CypherValue](
      CAPSRelationship(40, 1, 1, "", CypherMap("c" -> 10, "b" -> CypherNull))),
    Seq[CypherValue](CypherNull)
  )

  implicit lazy val NODE_valueGroups: ValueGroups[CypherValue] = Seq(
    Seq[CypherValue](
      CAPSNode(1, Set("Person"), CypherMap("a" -> 1, "b" -> CypherNull)),
      CAPSNode(1, Set("Person"), CypherMap("a" -> 1.0d, "b" -> CypherNull))
    ),
    Seq[CypherValue](CAPSNode(10, Set(), CypherMap("a" -> 1))),
    Seq[CypherValue](
      CAPSNode(20, Set("MathGuy"), CypherMap("a" -> 1, "b" -> 1))),
    Seq[CypherValue](
      CAPSNode(21, Set("MathGuy", "FanOfNulls"), CypherMap("b" -> CypherNull))),
    Seq[CypherValue](CAPSNode(30, Set("NoOne"), CypherMap.empty)),
    Seq[CypherValue](CAPSNode(40, Set(), CypherMap("c" -> 10, "b" -> CypherNull))),
    Seq[CypherValue](CypherNull)
  )

  implicit lazy val MAP_valueGroups: ValueGroups[CypherValue] = Seq(
    // TODO: Add more nested examples
    Seq[CypherValue](CypherMap.empty),
    Seq[CypherValue](CypherMap("a" -> 1L)),
    Seq[CypherValue](CypherMap("a" -> 1L, "b" -> 1L)),
    Seq[CypherValue](
      CypherMap("a" -> 1L, "b" -> CypherNull),
      CypherMap("a" -> 1.0d, "b" -> CypherNull)
    ),
    Seq[CypherValue](CypherMap("b" -> CypherNull)),
    Seq[CypherValue](CypherMap("c" -> 10L, "b" -> CypherNull)),
    Seq[CypherValue](CypherNull)
  )

  implicit lazy val LIST_valueGroups: ValueGroups[CypherValue] = Seq(
    // TODO: Add more nested examples
    Seq[CypherValue](CypherList.empty),
    Seq[CypherValue](CypherList(1)),
    Seq[CypherValue](CypherList(1, 0)),
    Seq[CypherValue](CypherList(1, 0, 2)),
    Seq[CypherValue](CypherList(1, 0.5)),
    Seq[CypherValue](CypherList(1, 1.5)),
    Seq[CypherValue](CypherList(1, CypherNull, 2)),
    Seq[CypherValue](CypherNull)
  )

  implicit lazy val STRING_valueGroups: ValueGroups[CypherValue] = Seq(
    Seq[CypherValue](""),
    Seq[CypherValue]("  "),
    Seq[CypherValue]("1234567890"),
    Seq[CypherValue]("A"),
    Seq[CypherValue]("AB"),
    Seq[CypherValue]("ABC"),
    Seq[CypherValue]("Is it a query, if no one sees it running?"),
    Seq[CypherValue]("a", "a"),
    Seq[CypherValue](CypherNull)
  )

  implicit lazy val BOOLEAN_valueGroups: ValueGroups[CypherValue] = Seq(
    Seq[CypherValue](false, false),
    Seq[CypherValue](true),
    Seq[CypherValue](CypherNull)
  )

  implicit lazy val INTEGER_valueGroups: ValueGroups[CypherValue] = Seq(
    Seq[CypherValue](Long.MinValue),
    Seq[CypherValue](-23L),
    Seq[CypherValue](-10L, -10L),
    Seq[CypherValue](-1L),
    Seq[CypherValue](0L),
    Seq[CypherValue](1L),
    Seq[CypherValue](2L),
    Seq[CypherValue](5L, 5L),
    Seq[CypherValue](42L),
    Seq[CypherValue](Long.MaxValue),
    Seq[CypherValue](CypherNull, CypherNull)
  )

  implicit lazy val FLOAT_valueGroups: ValueGroups[CypherValue] = Seq(
    Seq[CypherValue](Double.NegativeInfinity),
    Seq[CypherValue](Double.MinValue),
    Seq[CypherValue](-23.0d),
    Seq[CypherValue](-10.0d, -10.0d),
    Seq[CypherValue](0.0d),
    Seq[CypherValue](2.3d),
    Seq[CypherValue](5.0d),
    Seq[CypherValue](5.1d, 5.1d),
    Seq[CypherValue](42.0d),
    Seq[CypherValue](Double.MaxValue),
    Seq[CypherValue](Double.PositiveInfinity),
    Seq[CypherValue](Double.NaN),
    Seq[CypherValue](CypherNull)
  )

  implicit lazy val NUMBER_valueGroups: ValueGroups[CypherValue] = Seq(
    Seq[CypherValue](Double.NegativeInfinity),
    Seq[CypherValue](Double.MinValue),
    Seq[CypherValue](Long.MinValue),
    Seq[CypherValue](-23L, -23.0d),
    Seq[CypherValue](-10.0d, -10L, -10.0d, -10L),
    Seq[CypherValue](-1L, -1.0d),
    Seq[CypherValue](0L, 0.0d),
    Seq[CypherValue](1L),
    Seq[CypherValue](2L),
    Seq[CypherValue](2.3d),
    Seq[CypherValue](5L, 5L, 5.0d),
    Seq[CypherValue](5.1d, 5.1d),
    Seq[CypherValue](42.0d, 42L),
    Seq[CypherValue](Long.MaxValue),
    Seq[CypherValue](Double.MaxValue),
    Seq[CypherValue](Double.PositiveInfinity),
    Seq[CypherValue](Double.NaN),
    Seq[CypherValue](CypherNull, CypherNull)
  )

  implicit lazy val ANY_valueGroups: ValueGroups[CypherValue] = {
    val allGroups: Seq[ValueGroups[CypherValue]] = Seq(
      MAP_valueGroups,
      NODE_valueGroups,
      RELATIONSHIP_valueGroups,
      //        PATH_valueGroups,
      LIST_valueGroups,
      STRING_valueGroups,
      BOOLEAN_valueGroups,
      NUMBER_valueGroups
    )

    val materials: ValueGroups[CypherValue] =
      allGroups.flatMap(_.materialValueGroups)
    val nulls: ValueGroups[CypherValue] = Seq(
      allGroups.flatMap(_.CypherNullableValueGroups).flatten)

    materials ++ nulls
  }

  implicit final class CypherValueGroups[V <: CypherValue](
    elements: ValueGroups[V]) {

    def materialValueGroups: ValueGroups[V] =
      elements.map(_.filterNot(_.isNull)).filter(_.nonEmpty)

    def CypherNullableValueGroups: ValueGroups[V] =
      elements.map(_.filter(_.isNull)).filter(_.nonEmpty)

  }

}
