/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.opencypher.okapi.api.value.CypherValue.{CypherBoolean, CypherEntity, CypherFloat, CypherInteger, CypherList, CypherMap, CypherNode, CypherPath, CypherRelationship, CypherString}
import org.scalatest.{FunSpec, Matchers}

class CypherValueTest extends FunSpec with Matchers {
  describe("#toCypherString") {
    it("converts literals") {
      val mapping = Map(
        new CypherString("") -> "''",
        new CypherString("foo") -> "'foo'",
        new CypherString("a'b") -> "'a\\'b'",
        new CypherString("a\\b") -> "'a\\\\b'",
        new CypherInteger(1L) -> "1",
        new CypherFloat(3.14) -> "3.14",
        new CypherBoolean(true) -> "true"
      )

      mapping.foreach {
        case (input, expected) => input.toCypherString should equal(expected)
      }
    }

    it("converts a CypherList") {
      CypherList("foo", 123, false).toCypherString should equal("['foo', 123, false]")
      CypherList().toCypherString should equal("[]")
    }

    it("converts a CypherMap") {
      CypherMap("foo" -> "bar", "foo\\bar" -> 42, "foo\"bar" -> false).toCypherString should equal(
        "{`foo`: 'bar', `foo\\\"bar`: false, `foo\\\\bar`: 42}"
      )
      CypherMap().toCypherString should equal("{}")
    }

    it("converts a CypherRelationship") {
      val mapping = Map(
        TestRelationship(1, 1, 2, "REL", CypherMap("foo" -> 42)) -> "[:`REL` {`foo`: 42}]",
        TestRelationship(1, 1, 2, "REL") -> "[:`REL`]",
        TestRelationship(1, 1, 2, "My'Rel", CypherMap("foo" -> 42)) -> "[:`My\\'Rel` {`foo`: 42}]"
      )

      mapping.foreach {
        case (input, expected) => input.toCypherString should equal(expected)
      }
    }

    it("converts a CypherNode") {
      val mapping = Map(
        TestNode(1, Set("A"), CypherMap("foo" -> 42)) -> "(:`A` {`foo`: 42})",
        TestNode(1) -> "()",
        TestNode(1, Set("My\"Node", "My'Node"), CypherMap("foo" -> 42)) -> "(:`My\\\"Node`:`My\\\'Node` {`foo`: 42})"
      )

      mapping.foreach {
        case (input, expected) => input.toCypherString should equal(expected)
      }
    }

    it("converts a CypherPath") {
      val node = TestNode(1, Set("A"), CypherMap("foo" -> 42))
      val rel = TestRelationship(1, 1, 1, "REL", CypherMap("foo" -> 42))

      val mapping = Map(
        TestPath(Seq(node, rel, node)) -> s"<${node.toCypherString}--${rel.toCypherString}--${node.toCypherString}>",
        TestPath(Seq.empty) -> "<>"
      )

      mapping.foreach {
        case (input, expected) => input.toCypherString should equal(expected)
      }
    }
  }

  private case class TestRelationship(override val id: Long,
                                      override val startId: Long,
                                      override val endId: Long,
                                      override val relType: String,
                                      override val properties: CypherMap =
                                        CypherMap.empty)
      extends CypherRelationship[Long] {

    override type I = TestRelationship

    override def copy(id: Long = id,
                      startId: Long = startId,
                      endId: Long = endId,
                      relType: String = relType,
                      properties: CypherMap = properties): TestRelationship = {
      TestRelationship(id, startId, endId, relType, properties)
        .asInstanceOf[this.type]
    }
  }

  case class TestNode(override val id: Long,
                      override val labels: Set[String] = Set.empty,
                      override val properties: CypherMap = CypherMap.empty)
      extends CypherNode[Long] {

    override type I = TestNode

    override def copy(id: Long = id,
                      labels: Set[String] = labels,
                      properties: CypherMap = properties): TestNode = {
      TestNode(id, labels, properties)
    }

  }

  case class TestPath(override val value: Seq[CypherEntity[Long]]) extends CypherPath[Long]

}
