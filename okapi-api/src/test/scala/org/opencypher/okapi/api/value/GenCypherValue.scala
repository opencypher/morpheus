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
package org.opencypher.okapi.api.value

import org.opencypher.okapi.api.value.CypherValue._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen.{choose, const, listOfN, lzy, mapOfN, oneOf}

object GenCypherValue {

  val maxContainerSize: Int = 3

  val string: Gen[CypherString] = arbitrary[String]
    .map(s => CypherString(s.replace(''', '"')))

  def oneOfSeq[T](gs: Seq[Gen[T]]): Gen[T] = choose(0, gs.size - 1).flatMap(gs(_))

  val boolean: Gen[CypherBoolean] = arbitrary[Boolean].map(CypherBoolean)
  val integer: Gen[CypherInteger] = arbitrary[Long].map(CypherInteger)
  val float: Gen[CypherFloat] = arbitrary[Double].map(CypherFloat)
  val number: Gen[CypherNumber[Any]] = oneOf(integer, float)

  val labels: Gen[Set[String]] = arbitrary[Set[String]]

  val scalarGenerators: Seq[Gen[CypherValue]] = List(string, boolean, integer, float)

  val scalar: Gen[CypherValue] = oneOfSeq(scalarGenerators)

  val scalarOrNull: Gen[CypherValue] = oneOfSeq(scalarGenerators :+ const(CypherNull))

  val homogeneousScalarListGenerator: Gen[CypherList] = oneOfSeq(scalarGenerators.map(listWithElementGenerator))

  def nodeWithIdGenerator[Id](idGenerator: Gen[Id]): Gen[CypherNode[Id]] = lzy(for {
    id <- idGenerator
    ls <- labels
    ps <- map
  } yield TestNode(ls, ps)(id))

  def listWithElementGenerator(elementGeneratorGenerator: Gen[CypherValue]): Gen[CypherList] = lzy(for {
    size <- choose(min = 0, max = maxContainerSize)
    elementGenerator <- elementGeneratorGenerator
    listElements <- listOfN(size, elementGenerator)
  } yield listElements)

  lazy val any: Gen[CypherValue] = lzy(oneOf(scalarOrNull, map, list, node, relationship))

  lazy val list: Gen[CypherList] = lzy(listWithElementGenerator(any))

  lazy val propertyMap: Gen[CypherMap] = mapWithValueGenerator(oneOfSeq(scalarGenerators :+ homogeneousScalarListGenerator))

  lazy val map: Gen[CypherMap] = lzy(mapWithValueGenerator(any))

  def mapWithValueGenerator(valueGenerator: Gen[CypherValue]): Gen[CypherMap] = lzy(for {
    size <- choose(min = 0, max = maxContainerSize)
    keyValuePairs <- mapOfN(size, for {
      key <- arbitrary[String]
      value <- valueGenerator
    } yield key -> value)
  } yield keyValuePairs)

  val node: Gen[CypherNode[CypherValue]] = for {
    id <- scalar
    ls <- labels
    ps <- propertyMap
  } yield TestNode(ls, ps)(id)

  val relationship: Gen[CypherRelationship[CypherValue]] = for {
    id <- scalar
    start <- scalar
    end <- scalar
    relType <- arbitrary[String]
    ps <- propertyMap
  } yield TestRelationship(relType, ps)(id, start, end)

  // TODO: Add date and datetime generators

  case class TestNode[Id](
    labels: Set[String],
    properties: CypherMap
  )(
    val id: Id
  ) extends CypherNode[Id] {
    override type I = TestNode[Id]
    override def copy(
      id: Id,
      labels: Set[String],
      properties: CypherMap
    ): TestNode[Id] = TestNode(labels, properties)(id)
  }

  object TestNode {

    def apply[Id](
      id: Id,
      labels: Set[String] = Set.empty,
      properties: CypherMap = CypherMap.empty
    ): TestNode[Id] = TestNode(labels, properties)(id)

  }

  case class TestRelationship[Id](
    relType: String,
    properties: CypherMap
  )(
    val id: Id,
    val startId: Id,
    val endId: Id
  ) extends CypherRelationship[Id] {
    override type I = TestRelationship[Id]
    override def copy(
      id: Id,
      startId: Id,
      endId: Id,
      relType: String,
      properties: CypherMap
    ): TestRelationship[Id] = TestRelationship(relType, properties)(id, startId, endId)
  }

  object TestRelationship {

    def apply[Id](
      id: Id,
      startId: Id,
      endId: Id,
      relType: String,
      properties: CypherMap = CypherMap.empty
    ): TestRelationship[Id] = TestRelationship(relType, properties)(id, startId, endId)

  }

}
