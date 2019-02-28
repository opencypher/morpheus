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
import org.opencypher.okapi.api.value.CypherValue.Format.defaultValueFormatter
import org.opencypher.okapi.impl.exception.IllegalArgumentException
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

  def nodeWithIdGenerator[Id](idGenerator: Gen[Id]): Gen[TestNode[Id]] = lzy(for {
    id <- idGenerator
    ls <- labels
    ps <- map
  } yield TestNode(id, ls, ps))

  val node: Gen[TestNode[CypherInteger]] = nodeWithIdGenerator(integer)

  def relationshipWithIdGenerators[Id](
    relId: Gen[Id],
    startNodeId: Gen[Id],
    endNodeId: Gen[Id]
  ): Gen[TestRelationship[Id]] = {
    for {
      id <- relId
      start <- startNodeId
      end <- endNodeId
      relType <- arbitrary[String]
      ps <- propertyMap
    } yield TestRelationship(id, start, end, relType, ps)
  }

  val relationship: Gen[TestRelationship[CypherInteger]] = relationshipWithIdGenerators(integer, integer, integer)

  // TODO: Add date and datetime generators

  case class NodeRelNodePattern[Id](
    startNode: TestNode[Id],
    relationship: TestRelationship[Id],
    endNode: TestNode[Id]
  ) {
    def toCreateQuery: String = {
      s"CREATE ${startNode.toCypherString}-${relationship.toCypherString}->${endNode.toCypherString}"
    }
  }

  val nodeRelNodePattern: Gen[NodeRelNodePattern[_]] = {
    val n1Id = 0L
    val rId = 0L
    val n2Id = 1L
    for {
      startNode <- nodeWithIdGenerator(const(n1Id))
      relationship <- relationshipWithIdGenerators(const(rId), const(n1Id), const(n2Id))
      endNode <- nodeWithIdGenerator(const(n2Id))
    } yield NodeRelNodePattern(startNode, relationship, endNode)
  }

  case class TestNode[Id](
    id: Id,
    labels: Set[String] = Set.empty,
    properties: CypherMap = CypherMap.empty
  ) extends CypherNode[Id] {
    override type I = TestNode[Id]
    override def copy(
      id: Id,
      labels: Set[String],
      properties: CypherMap
    ): TestNode[Id] = TestNode(id, labels, properties)

    override def productArity: Int = 2

    override def productElement(n: Int): Any = n match {
      case 0 => labels
      case 1 => properties
      case other => throw IllegalArgumentException("a valid product index", s"$other")
    }

    override def toString = s"${getClass.getSimpleName}($labels, $properties)}"
  }

  object TestNode {

    def apply[Id](n: CypherNode[Id]): TestNode[Id] = {
      TestNode(n.id, n.labels, n.properties)
    }

  }

  case class TestRelationship[Id](
    id: Id,
    startId: Id,
    endId: Id,
    relType: String,
    properties: CypherMap = CypherMap.empty
  ) extends CypherRelationship[Id] {
    override type I = TestRelationship[Id]
    override def copy(
      id: Id,
      startId: Id,
      endId: Id,
      relType: String,
      properties: CypherMap
    ): TestRelationship[Id] = TestRelationship(id, startId, endId, relType, properties)

    override def productArity: Int = 2

    override def productElement(n: Int): Any = n match {
      case 0 => relType
      case 1 => properties
      case other => throw IllegalArgumentException("a valid product index", s"$other")
    }

    override def toString = s"${getClass.getSimpleName}($relType, $properties)}"
  }

  object TestRelationship {

    def apply[Id](r: CypherRelationship[Id]): TestRelationship[Id] = {
      TestRelationship(r.id, r.startId, r.endId, r.relType, r.properties)
    }

  }

}
