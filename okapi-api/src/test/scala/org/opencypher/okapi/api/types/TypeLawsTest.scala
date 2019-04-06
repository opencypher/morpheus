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
package org.opencypher.okapi.api.types

import cats.Monoid
import cats.kernel.Eq
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.{FunSuite, Matchers}
import org.typelevel.discipline.scalatest.Discipline

class TypeLawsTest extends FunSuite with Matchers with ScalaCheckDrivenPropertyChecks with Discipline {

  def pickOne[T](gens: List[Gen[T]]): Gen[T] = for {
    i <- Gen.choose(0, gens.size - 1)
    t <- gens(i)
  } yield t

  val maybeGraph: Gen[Option[QualifiedGraphName]] = Gen.oneOf(None, Some(QualifiedGraphName("ns.g1")), Some(QualifiedGraphName("ns.g2")))

  val nodeLabel: Gen[String] = Gen.oneOf("A", "B", "C")

  val bigDecimalType: Gen[CTBigDecimal] = for {
    precision <- Gen.posNum[Int]
    scale <- Gen.posNum[Int]
  } yield CTBigDecimal(precision, scale)

  val node: Gen[CTNode] = for {
    labels <- Gen.listOf(nodeLabel)
    g <- maybeGraph
  } yield CTNode(labels.toSet, g)

  val relType: Gen[String] = Gen.oneOf("R1", "R2", "R3")

  val relationship: Gen[CTRelationship] = for {
    relTypes <- Gen.listOf(relType)
    g <- maybeGraph
  } yield CTRelationship(relTypes.toSet, g)

  val flatTypes: List[Gen[CypherType]] = List(
    Gen.const(CTVoid),
    Gen.const(CTNull),
    Gen.const(CTString),
    Gen.const(CTInteger),
    Gen.const(CTFloat),
    Gen.const(CTUnion(CTFloat, CTInteger, CTBigDecimal)),
    Gen.const(CTBigDecimal),
    Gen.const(CTBoolean),
    Gen.const(CTTrue),
    Gen.const(CTFalse),
    Gen.const(CTIdentity),
    Gen.const(CTLocalDateTime),
    Gen.const(CTDate),
    Gen.const(CTDuration),
    node,
    relationship,
    bigDecimalType,
    Gen.const(CTAny),
    Gen.const(CTAnyMaterial),
    Gen.const(CTNode),
    Gen.const(CTRelationship)
  )

  val flat: Gen[CypherType] = pickOne(flatTypes)

  val list: Gen[CTList] = for {
    elem <- flat
  } yield CTList(elem)

  val field: Gen[(String, CypherType)] = for {
    name <- Gen.identifier
    elem <- flat
  } yield name -> elem

  val map: Gen[CTMap] = for {
    fields <- Gen.mapOf(field)
  } yield CTMap(fields)

  val nestedTypes: List[Gen[CypherType]] = List(list, map, Gen.const(CTList), Gen.const(CTMap))

  val allTypes: List[Gen[CypherType]] = List(flatTypes, nestedTypes).flatten

  val anyType: Gen[CypherType] = pickOne(allTypes)

  implicit val arbitraryCypherType: Arbitrary[CypherType] = Arbitrary(anyType)

  implicit val eq: Eq[CypherType] = Eq.fromUniversalEquals

  val unionMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    def empty: CypherType = CTVoid
    def combine(x: CypherType, y: CypherType): CypherType = x | y
  }

  val intersectionMonoid: Monoid[CypherType] = new Monoid[CypherType] {
    def empty: CypherType = CTAny
    def combine(x: CypherType, y: CypherType): CypherType = x & y
  }

  checkAll("CypherType.union", cats.kernel.laws.discipline.MonoidTests[CypherType](unionMonoid).monoid)

  checkAll("CypherType.intersection", cats.kernel.laws.discipline.MonoidTests[CypherType](intersectionMonoid).monoid)

}
