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

import org.opencypher.caps.api.value._
import org.opencypher.caps.api.value.CypherValue._

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

  //  implicit val PATH_valueGroups: ValueGroups[CAPSPath] = Seq(
  //    Seq(
  //      CAPSPath(Seq(CAPSNode(1l, Array("Label"), Properties.empty))),
  //      CAPSPath(Seq(CAPSNode(1l, Array("NotSignificant"), Properties.empty))),
  //      CAPSPath(Seq(CAPSNode(1l, Array("NotSignificant"), Properties("alsoNotSig" -> CAPSBoolean(true)))))
  //    ),
  //    Seq(
  //      CAPSPath(
  //        Seq(
  //          CAPSNode(1l, Array("Label"), Properties.empty),
  //          CAPSRelationship(100l, 1l, 2l, "KNOWS", Properties.empty),
  //          CAPSNode(2l, Seq.empty, Properties.empty))),
  //      CAPSPath(
  //        Seq(
  //          CAPSNode(1l, Seq("Label"), Properties.empty),
  //          CAPSRelationship(100l, 1l, 2l, "FORGETS", Properties.empty),
  //          CAPSNode(2l, Seq.empty, Properties.empty)))
  //    ),
  //    Seq(
  //      CAPSPath(Seq(
  //        CAPSNode(1l, Seq("Label"), Properties.empty),
  //        CAPSRelationship(100l, 1l, 2l, "KNOWS", Properties("aRelProp" -> 667.5)),
  //        CAPSNode(2l, Seq.empty, Properties.empty),
  //        CAPSRelationship(100l, 1l, 2l, "KNOWS", Properties.empty),
  //        CAPSNode(2l, Seq("One", "Two", "Three"), Properties.empty)
  //      ))),
  //    Seq(cypherNull[CAPSPath])
  //  )

  implicit val RELATIONSHIP_valueGroups: ValueGroups[NullableCypherRelationship[Long]] = Seq(
    Seq(
      CAPSRelationship(
        1,
        1,
        1,
        "KNOWS",
        Properties("a" -> 1, "b" -> null)),
      CAPSRelationship(
        1,
        2,
        4,
        "FORGETS",
        Properties("a" -> 1.0, "b" -> null))
    ),
    Seq(CAPSRelationship(10, 1, 1, "KNOWS", Properties("a" -> 1))),
    Seq(
      CAPSRelationship(
        20,
        1,
        1,
        "KNOWS",
        Properties("a" -> 1, "b" -> 1))),
    Seq(CAPSRelationship(21, 0, -1, "KNOWS", Properties("b" -> null))),
    Seq(CAPSRelationship(30, 1, 1, "_-&", Properties.empty)),
    Seq(
      CAPSRelationship(
        40,
        1,
        1,
        "",
        Properties("c" -> 10, "b" -> null))),
    Seq(CypherNull)
  )

  implicit val NODE_valueGroups: ValueGroups[NullableCypherNode[Long]] = Seq(
    Seq(
      CAPSNode(1, Set("Person"), Properties("a" -> 1, "b" -> null)),
      CAPSNode(1, Set("Person"), Properties("a" -> 1.0d, "b" -> null))
    ),
    Seq(CAPSNode(10, Set(), Properties("a" -> 1))),
    Seq(CAPSNode(20, Set("MathGuy"), Properties("a" -> 1, "b" -> 1))),
    Seq(CAPSNode(21, Set("MathGuy", "FanOfNulls"), Properties("b" -> null))),
    Seq(CAPSNode(30, Set("NoOne"), Properties.empty)),
    Seq(CAPSNode(40, Set(), Properties("c" -> 10, "b" -> null))),
    Seq(CypherNull)
  )

  implicit val MAP_valueGroups: ValueGroups[NullableCypherMap] = Seq(
    // TODO: Add more nested examples
    Seq(CypherMap()),
    Seq(CypherMap("a" -> 1)),
    Seq(CypherMap("a" -> 1, "b" -> 1)),
    Seq(
      CypherMap("a" -> 1, "b" -> null),
      CypherMap("a" -> 1.0d, "b" -> null)
    ),
    Seq(CypherMap("b" -> null)),
    Seq(CypherMap("c" -> 10, "b" -> null)),
    Seq(CypherNull)
  )

  implicit val LIST_valueGroups: ValueGroups[NullableCypherList[_]] = Seq(
    // TODO: Add more nested examples
    Seq(CypherList()),
    Seq(CypherList(1)),
    Seq(CypherList(1, 0)),
    Seq(CypherList(1, 0, 2)),
    Seq(CypherList(1, 0.5)),
    Seq(CypherList(1, 1.5)),
    Seq(CypherList(1, CypherNull, 2)),
    Seq(CypherNull)
  )

  implicit val STRING_valueGroups: ValueGroups[NullableCypherString] = Seq(
    Seq(""),
    Seq("  "),
    Seq("1234567890"),
    Seq("A"),
    Seq("AB"),
    Seq("ABC"),
    Seq("Is it a query, if no one sees it running?"),
    Seq("a", "a"),
    Seq(CypherNull)
  )

  implicit val BOOLEAN_valueGroups: ValueGroups[NullableCypherBoolean] = Seq(
    Seq(false, false),
    Seq(true),
    Seq(CypherNull)
  )

  implicit val INTEGER_valueGroups: ValueGroups[NullableCypherInteger] = Seq(
    Seq(Long.MinValue),
    Seq(-23L),
    Seq(-10, -10),
    Seq(-1),
    Seq(0),
    Seq(1),
    Seq(2),
    Seq(5, 5),
    Seq(42L),
    Seq(Long.MaxValue),
    Seq(CypherNull, CypherNull)
  )

  implicit val FLOAT_valueGroups: ValueGroups[NullableCypherFloat] = Seq(
    Seq(Double.NegativeInfinity),
    Seq(Double.MinValue),
    Seq(-23.0d),
    Seq(-10.0d, -10.0d),
    Seq(0.0d),
    Seq(2.3d),
    Seq(5.0d),
    Seq(5.1d, 5.1d),
    Seq(42.0d),
    Seq(Double.MaxValue),
    Seq(Double.PositiveInfinity),
    Seq(Double.NaN),
    Seq(CypherNull)
  )

  implicit val NUMBER_valueGroups: ValueGroups[NullableCypherNumber] = Seq(
    Seq(Double.NegativeInfinity),
    Seq(Double.MinValue),
    Seq(Long.MinValue),
    Seq(-23L, -23.0d),
    Seq(-10.0d, -10, -10.0d, -10),
    Seq(-1, -1.0d),
    Seq(0, 0.0d),
    Seq(1),
    Seq(2),
    Seq(2.3d),
    Seq(5, 5, 5.0d),
    Seq(5.1d, 5.1d),
    Seq(42.0d, 42L),
    Seq(Long.MaxValue),
    Seq(Double.MaxValue),
    Seq(Double.PositiveInfinity),
    Seq(Double.NaN),
    Seq(CypherNull, CypherNull, CypherNull)
  )

//  implicit val ANY_valueGroups: ValueGroups[NullableCypherValue[_]] = {
//    val allGroups: Seq[ValueGroups[NullableCypherValue[_]]] = Seq(
//      MAP_valueGroups: ValueGroups[NullableCypherValue[_]],
//      NODE_valueGroups: ValueGroups[NullableCypherValue[_]],
//      RELATIONSHIP_valueGroups: ValueGroups[NullableCypherValue[_]],
//      //      PATH_valueGroups,
//      LIST_valueGroups: ValueGroups[NullableCypherValue[_]],
//      STRING_valueGroups: ValueGroups[NullableCypherValue[_]],
//      BOOLEAN_valueGroups: ValueGroups[NullableCypherValue[_]],
//      NUMBER_valueGroups: ValueGroups[NullableCypherValue[_]]
//    )
//
//    val materials = allGroups.flatMap(_.flatMap(_.as[MaterialCypherValue]))
//    val nulls = Seq(allGroups.flatMap(_.nullableValueGroups).flatten)
//
//    materials ++ nulls
//  }

//  implicit final class CypherValueGroups[V <: NullableCypherValue[_]](elts: ValueGroups[V]) {
//
//    def materialValueGroups: ValueGroups[V] = elts.map(_.filter(_ != CypherNull).flatMap(_.as[V])).filter(_.nonEmpty)
//
//    def nullableValueGroups: ValueGroups[V] = elts.map(_.filter(_ == CypherNull)).filter(_.nonEmpty)
//
//    def scalaValueGroups: Seq[Seq[Any]] = elts.map(group => group.map(_.value))
//
//    def indexed: Seq[(Int, V)] =
//      elts.zipWithIndex.flatMap {
//        case ((group), index) =>
//          group.map { v =>
//            index -> v
//          }
//      }
//  }

  //  implicit final class CypherValues[V <: MaterialCypherValue](elts: Values[V]) {
  //    def scalaValues(implicit companion: CAPSValueCompanion[V]): Seq[Option[companion.Contents]] =
  //      elts.map { value =>
  //        companion.contents(value)
  //      }
  //  }
}
