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

import org.opencypher.caps.api.value.instances._
import org.opencypher.caps.api.value.syntax._

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

  implicit val PATH_valueGroups: ValueGroups[CAPSPath] = Seq(
    Seq(
      CAPSPath(Seq(CAPSNode(1l, Array("Label"), Properties.empty))),
      CAPSPath(Seq(CAPSNode(1l, Array("NotSignificant"), Properties.empty))),
      CAPSPath(Seq(CAPSNode(1l, Array("NotSignificant"), Properties("alsoNotSig" -> CAPSBoolean(true)))))
    ),
    Seq(
      CAPSPath(
        Seq(
          CAPSNode(1l, Array("Label"), Properties.empty),
          CAPSRelationship(100l, 1l, 2l, "KNOWS", Properties.empty),
          CAPSNode(2l, Seq.empty, Properties.empty))),
      CAPSPath(
        Seq(
          CAPSNode(1l, Seq("Label"), Properties.empty),
          CAPSRelationship(100l, 1l, 2l, "FORGETS", Properties.empty),
          CAPSNode(2l, Seq.empty, Properties.empty)))
    ),
    Seq(
      CAPSPath(Seq(
        CAPSNode(1l, Seq("Label"), Properties.empty),
        CAPSRelationship(100l, 1l, 2l, "KNOWS", Properties("aRelProp" -> CAPSFloat(667.5))),
        CAPSNode(2l, Seq.empty, Properties.empty),
        CAPSRelationship(100l, 1l, 2l, "KNOWS", Properties.empty),
        CAPSNode(2l, Seq("One", "Two", "Three"), Properties.empty)
      ))),
    Seq(cypherNull[CAPSPath])
  )

  implicit val RELATIONSHIP_valueGroups: ValueGroups[CAPSRelationship] = Seq(
    Seq(
      CAPSRelationship(
        EntityId(1),
        EntityId(1),
        EntityId(1),
        "KNOWS",
        Properties("a" -> CAPSInteger(1), "b" -> null)),
      CAPSRelationship(
        EntityId(1),
        EntityId(2),
        EntityId(4),
        "FORGETS",
        Properties("a" -> CAPSFloat(1.0), "b" -> null))
    ),
    Seq(CAPSRelationship(EntityId(10), EntityId(1), EntityId(1), "KNOWS", Properties("a" -> CAPSInteger(1)))),
    Seq(
      CAPSRelationship(
        EntityId(20),
        EntityId(1),
        EntityId(1),
        "KNOWS",
        Properties("a" -> CAPSInteger(1), "b" -> CAPSInteger(1)))),
    Seq(CAPSRelationship(EntityId(21), EntityId(0), EntityId(-1), "KNOWS", Properties("b" -> null))),
    Seq(CAPSRelationship(EntityId(30), EntityId(1), EntityId(1), "_-&", Properties.empty)),
    Seq(
      CAPSRelationship(
        EntityId(40),
        EntityId(1),
        EntityId(1),
        "",
        Properties("c" -> CAPSInteger(10), "b" -> null))),
    Seq(cypherNull[CAPSRelationship])
  )

  implicit val NODE_valueGroups: ValueGroups[CAPSNode] = Seq(
    Seq(
      CAPSNode(EntityId(1), Seq("Person"), Properties("a" -> CAPSInteger(1), "b" -> null)),
      CAPSNode(EntityId(1), Seq("Person"), Properties("a" -> CAPSFloat(1.0d), "b" -> null))
    ),
    Seq(CAPSNode(EntityId(10), Seq(), Properties("a" -> CAPSInteger(1)))),
    Seq(CAPSNode(EntityId(20), Seq("MathGuy"), Properties("a" -> CAPSInteger(1), "b" -> CAPSInteger(1)))),
    Seq(CAPSNode(EntityId(21), Seq("MathGuy", "FanOfNulls"), Properties("b" -> null))),
    Seq(CAPSNode(EntityId(30), Seq("NoOne"), Properties.empty)),
    Seq(CAPSNode(EntityId(40), Seq(), Properties("c" -> CAPSInteger(10), "b" -> null))),
    Seq(cypherNull[CAPSNode])
  )

  implicit val MAP_valueGroups: ValueGroups[CAPSMap] = Seq(
    // TODO: Add more nested examples
    Seq(CAPSMap()),
    Seq(CAPSMap("a" -> CAPSInteger(1))),
    Seq(CAPSMap("a" -> CAPSInteger(1), "b" -> CAPSInteger(1))),
    Seq(
      CAPSMap("a" -> CAPSInteger(1), "b" -> null),
      CAPSMap("a" -> CAPSFloat(1.0d), "b" -> null)
    ),
    Seq(CAPSMap("b" -> null)),
    Seq(CAPSMap("c" -> CAPSInteger(10), "b" -> null)),
    Seq(cypherNull[CAPSMap])
  )

  implicit val LIST_valueGroups: ValueGroups[CAPSList] = Seq(
    // TODO: Add more nested examples
    Seq(CAPSList(Seq())),
    Seq(CAPSList(Seq(CAPSInteger(1)))),
    Seq(CAPSList(Seq(CAPSInteger(1), CAPSInteger(0)))),
    Seq(CAPSList(Seq(CAPSInteger(1), CAPSFloat(0), CAPSInteger(2)))),
    Seq(CAPSList(Seq(CAPSInteger(1), CAPSFloat(0.5)))),
    Seq(CAPSList(Seq(CAPSInteger(1), CAPSFloat(1.5)))),
    Seq(CAPSList(Seq(CAPSInteger(1), cypherNull[CAPSNumber], CAPSInteger(2)))),
    Seq(cypherNull[CAPSList])
  )

  implicit val STRING_valueGroups: ValueGroups[CAPSString] = Seq(
    Seq(CAPSString("")),
    Seq(CAPSString("  ")),
    Seq(CAPSString("1234567890")),
    Seq(CAPSString("A")),
    Seq(CAPSString("AB")),
    Seq(CAPSString("ABC")),
    Seq(CAPSString("Is it a query, if no one sees it running?")),
    Seq(CAPSString("a"), CAPSString("a")),
    Seq(cypherNull[CAPSString])
  )

  implicit val BOOLEAN_valueGroups: ValueGroups[CAPSBoolean] = Seq(
    Seq(CAPSBoolean(false), CAPSBoolean(false)),
    Seq(CAPSBoolean(true)),
    Seq(cypherNull[CAPSBoolean])
  )

  implicit val INTEGER_valueGroups: ValueGroups[CAPSInteger] = Seq(
    Seq(CAPSInteger(Long.MinValue)),
    Seq(CAPSInteger(-23L)),
    Seq(CAPSInteger(-10), CAPSInteger(-10)),
    Seq(CAPSInteger(-1)),
    Seq(CAPSInteger(0)),
    Seq(CAPSInteger(1)),
    Seq(CAPSInteger(2)),
    Seq(CAPSInteger(5), CAPSInteger(5)),
    Seq(CAPSInteger(42L)),
    Seq(CAPSInteger(Long.MaxValue)),
    Seq(cypherNull[CAPSInteger], cypherNull[CAPSInteger])
  )

  implicit val FLOAT_valueGroups: ValueGroups[CAPSFloat] = Seq(
    Seq(CAPSFloat(Double.NegativeInfinity)),
    Seq(CAPSFloat(Double.MinValue)),
    Seq(CAPSFloat(-23.0d)),
    Seq(CAPSFloat(-10.0d), CAPSFloat(-10.0d)),
    Seq(CAPSFloat(0.0d)),
    Seq(CAPSFloat(2.3d)),
    Seq(CAPSFloat(5.0d)),
    Seq(CAPSFloat(5.1d), CAPSFloat(5.1d)),
    Seq(CAPSFloat(42.0d)),
    Seq(CAPSFloat(Double.MaxValue)),
    Seq(CAPSFloat(Double.PositiveInfinity)),
    Seq(CAPSFloat(Double.NaN)),
    Seq(cypherNull[CAPSFloat])
  )

  implicit val NUMBER_valueGroups: ValueGroups[CAPSNumber] = Seq(
    Seq(CAPSFloat(Double.NegativeInfinity)),
    Seq(CAPSFloat(Double.MinValue)),
    Seq(CAPSInteger(Long.MinValue)),
    Seq(CAPSInteger(-23L), CAPSFloat(-23.0d)),
    Seq(CAPSFloat(-10.0d), CAPSInteger(-10), CAPSFloat(-10.0d), CAPSInteger(-10)),
    Seq(CAPSInteger(-1), CAPSFloat(-1.0d)),
    Seq(CAPSInteger(0), CAPSFloat(0.0d)),
    Seq(CAPSInteger(1)),
    Seq(CAPSInteger(2)),
    Seq(CAPSFloat(2.3d)),
    Seq(CAPSInteger(5), CAPSInteger(5), CAPSFloat(5.0d)),
    Seq(CAPSFloat(5.1d), CAPSFloat(5.1d)),
    Seq(CAPSFloat(42.0d), CAPSInteger(42L)),
    Seq(CAPSInteger(Long.MaxValue)),
    Seq(CAPSFloat(Double.MaxValue)),
    Seq(CAPSFloat(Double.PositiveInfinity)),
    Seq(CAPSFloat(Double.NaN)),
    Seq(cypherNull[CAPSFloat], cypherNull[CAPSInteger], cypherNull[CAPSNumber])
  )

  implicit val ANY_valueGroups: ValueGroups[CAPSValue] = {
    val allGroups = Seq(
      MAP_valueGroups,
      NODE_valueGroups,
      RELATIONSHIP_valueGroups,
      PATH_valueGroups,
      LIST_valueGroups,
      STRING_valueGroups,
      BOOLEAN_valueGroups,
      NUMBER_valueGroups
    )

    val materials = allGroups.flatMap(_.materialValueGroups)
    val nulls = Seq(allGroups.flatMap(_.nullableValueGroups).flatten)

    materials ++ nulls
  }

  implicit final class CypherValueGroups[V <: CAPSValue](elts: ValueGroups[V]) {

    def materialValueGroups: ValueGroups[V] = elts.map(_.filter(_ != cypherNull[V])).filter(_.nonEmpty)
    def nullableValueGroups: ValueGroups[V] = elts.map(_.filter(_ == cypherNull[V])).filter(_.nonEmpty)

    def scalaValueGroups: Seq[Seq[Any]] =
      elts.map { group =>
        CypherValues[CAPSValue](group).scalaValues.map(_.orNull)
      }

    def indexed: Seq[(Int, V)] =
      elts.zipWithIndex.flatMap {
        case ((group), index) =>
          group.map { v =>
            index -> v
          }
      }
  }

  implicit final class CypherValues[V <: CAPSValue](elts: Values[V]) {
    def scalaValues(implicit companion: CAPSValueCompanion[V]): Seq[Option[companion.Contents]] =
      elts.map { value =>
        companion.contents(value)
      }
  }
}
