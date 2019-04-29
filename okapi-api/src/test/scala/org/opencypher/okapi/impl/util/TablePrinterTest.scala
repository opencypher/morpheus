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
package org.opencypher.okapi.impl.util

import org.opencypher.okapi.ApiBaseTest
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.Format._
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship, CypherValue}
import org.opencypher.okapi.impl.util.TablePrinter.toTable

class TablePrinterTest extends ApiBaseTest {

  it("prints empty header and data") {
    val header = Seq.empty
    val data = Seq.empty

    toTable(header, data) should equal(
      """|╔══════════════╗
         |║ (no columns) ║
         |╠══════════════╣
         |║ (empty row)  ║
         |╚══════════════╝
         |(no rows)
         |""".stripMargin)
  }

  it("prints empty table") {
    val header = Seq("column")
    val data = Seq.empty

    toTable(header, data) should equal(
      """|╔════════╗
         |║ column ║
         |╚════════╝
         |(no rows)
         |""".stripMargin)
  }

  it("prints single row") {
    val header = Seq("column")
    val data = Seq(Seq(1))

    toTable(header, data) should equal(
      """|╔════════╗
         |║ column ║
         |╠════════╣
         |║ 1      ║
         |╚════════╝
         |(1 row)
         |""".stripMargin)
  }

  it("prints n rows") {
    val header = Seq("column")
    val data = Seq(Seq(1), Seq(2))

    toTable(header, data) should equal(
      """|╔════════╗
         |║ column ║
         |╠════════╣
         |║ 1      ║
         |║ 2      ║
         |╚════════╝
         |(2 rows)
         |""".stripMargin)
  }

  it("prints simple scala values") {
    val header = Seq("String", "Integer", "Float", "Boolean")
    val data = Seq(Seq("foo", 42, 42.23, true))

    toTable(header, data) should equal(
      """|╔════════╤═════════╤═══════╤═════════╗
         |║ String │ Integer │ Float │ Boolean ║
         |╠════════╪═════════╪═══════╪═════════╣
         |║ foo    │ 42      │ 42.23 │ true    ║
         |╚════════╧═════════╧═══════╧═════════╝
         |(1 row)
         |""".stripMargin)
  }

  it("prints simple cypher values correctly") {

    val header = Seq("String", "Integer", "Float", "Boolean")
    val data = Seq(Seq(CypherValue("foo"), CypherValue(42), CypherValue(42.23), CypherValue(true)))

    implicit val f: CypherValue => String = v => v.toCypherString

    toTable(header, data) should equal(
      """|╔════════╤═════════╤═══════╤═════════╗
         |║ String │ Integer │ Float │ Boolean ║
         |╠════════╪═════════╪═══════╪═════════╣
         |║ 'foo'  │ 42      │ 42.23 │ true    ║
         |╚════════╧═════════╧═══════╧═════════╝
         |(1 row)
         |""".stripMargin)
  }

  it("prints nested cypher values correctly") {
    val n = TestNode(42, Set("A", "B"), CypherMap("prop" -> 42))
    val r = TestRelationship(42, 23, 84, "KNOWS", CypherMap("prop" -> "foo"))

    val header = Seq("n", "r")
    val data = Seq(Seq(n, r))

    implicit val f: CypherValue => String = v => v.toCypherString

    println(toTable(header, data))

    toTable(header, data) should equal(
      """|╔═════════════════════════╤════════════════════════════╗
         |║ n                       │ r                          ║
         |╠═════════════════════════╪════════════════════════════╣
         |║ (:`A`:`B` {`prop`: 42}) │ [:`KNOWS` {`prop`: 'foo'}] ║
         |╚═════════════════════════╧════════════════════════════╝
         |(1 row)
         |""".stripMargin
    )
  }

  case class TestNode(id: Long, labels: Set[String], properties: CypherMap) extends Node[Long] {
    override type I = TestNode
    override def copy(id: Long, labels: Set[String], properties: CypherMap): TestNode =
      copy(id, labels, properties)
  }

  case class TestRelationship(id: Long, startId: Long, endId: Long, relType: String, properties: CypherMap) extends Relationship[Long] {
    override type I = TestRelationship
    override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): TestRelationship =
      copy(id, source, target, relType, properties)
  }
}
