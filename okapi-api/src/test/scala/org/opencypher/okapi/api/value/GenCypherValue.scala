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
import org.scalacheck.{Arbitrary, Gen}

object GenCypherValue {

  case class TestNode(
    id: CypherValue,
    labels: Set[String],
    properties: CypherMap
  ) extends CypherNode[CypherValue] {
    override type I = TestNode
    override def copy(
      id: CypherValue,
      labels: Set[String],
      properties: CypherMap
    ): TestNode = TestNode(id, labels, properties)
  }

  case class TestRelationship(
    id: CypherValue, startId: CypherValue,
    endId: CypherValue,
    relType: String,
    properties: CypherMap
  ) extends CypherRelationship[CypherValue] {
    override type I = TestRelationship
    override def copy(
      id: CypherValue,
      startId: CypherValue,
      endId: CypherValue,
      relType: String,
      properties: CypherMap
    ): TestRelationship = TestRelationship(id, startId, endId, relType, properties)
  }

  lazy val string: Gen[CypherString] = Arbitrary.arbString.arbitrary
    .map(s => CypherString(s.replace(''', '"')))
  lazy implicit val arbString: Arbitrary[CypherString] = Arbitrary(string)

  lazy val boolean: Gen[CypherBoolean] = Arbitrary.arbBool.arbitrary.map(CypherBoolean)
  lazy implicit val arbBoolean: Arbitrary[CypherBoolean] = Arbitrary(boolean)

  lazy val integer: Gen[CypherInteger] = Arbitrary.arbLong.arbitrary.map(CypherInteger)
  lazy implicit val arbInteger: Arbitrary[CypherInteger] = Arbitrary(integer)

  lazy val float: Gen[CypherFloat] = Arbitrary.arbDouble.arbitrary.map(CypherFloat)
  lazy implicit val arbFloat: Arbitrary[CypherFloat] = Arbitrary(float)

  lazy val number: Gen[CypherNumber[Any]] = Gen.oneOf(integer, float)
  lazy implicit val arbNumber: Arbitrary[CypherNumber[Any]] = Arbitrary(number)

  lazy val keyValuePair: Gen[(String, CypherValue)] = for {
    key <- Arbitrary.arbString.arbitrary
    value <- scalarOrNull
  } yield key -> value
  lazy implicit val arbKeyValuePair: Arbitrary[(String, CypherValue)] = Arbitrary(keyValuePair)

  lazy val map: Gen[CypherMap] = Gen.lzy(Gen.listOf(keyValuePair).map(CypherMap(_: _*)))
  lazy implicit val arbMap: Arbitrary[CypherMap] = Arbitrary(map)

  lazy val labels: Gen[Set[String]] = Gen.lzy(Gen.listOf(Arbitrary.arbString.arbitrary).map(_.toSet))
  lazy implicit val arbLabels: Arbitrary[Set[String]] = Arbitrary(labels)

  lazy val scalar: Gen[CypherValue] = Gen.oneOf(string, boolean, integer, float)

  lazy val scalarOrNull: Gen[CypherValue] = Gen.oneOf(scalar, Gen.const(CypherNull))

  lazy val node: Gen[CypherNode[CypherValue]] = for {
    id <- scalar
    ls <- labels
    ps <- map
  } yield TestNode(id, ls, ps)
  lazy implicit val arbNode: Arbitrary[CypherNode[CypherValue]] = Arbitrary(node)

  lazy val relationship: Gen[CypherRelationship[CypherValue]] = for {
    id <- scalar
    start <- scalar
    end <- scalar
    relType <- Arbitrary.arbString.arbitrary
    ps <- map
  } yield TestRelationship(id, start, end, relType, ps)
  lazy implicit val arbRelationship: Arbitrary[CypherRelationship[CypherValue]] = Arbitrary(relationship)

  lazy val list: Gen[CypherList] = Gen.lzy(Gen.listOf(scalarOrNull).map(CypherList))
  lazy implicit val arbList: Arbitrary[CypherList] = Arbitrary(list)

  lazy val any: Gen[CypherValue] = Gen.lzy(Gen.oneOf(scalarOrNull, map, list))
  lazy implicit val arbAny: Arbitrary[CypherValue.CypherValue] = Arbitrary(any)

  // TODO: Add date/datetime generators

}
