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
package org.opencypher.spark.impl

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8

import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.impl.table.RecordsPrinter
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.GraphConstructionFixture

class CAPSRecordsPrinterTest extends CAPSTestSuite with GraphConstructionFixture {

  implicit val options: PrintOptions = PrintOptions.out

  it("prints the unit table") {
    // Given
    val records = CAPSRecords.unit()

    // When
    print(records)

    // Then
    getString should equal(
      """!+--------------+
         !| (no columns) |
         !+--------------+
         !| (empty row)  |
         !+--------------+
         !(1 rows)
         !""".stripMargin('!')
    )
  }

  it("prints a single column with no rows") {
    // Given
    val records = CAPSRecords.empty(headerOf('foo))

    // When
    print(records)

    // Then
    getString should equal(
      """!+-----+
         !| foo |
         !+-----+
         !(no rows)
         !""".stripMargin('!')
    )
  }

  it("prints a single column with three rows") {
    // Given
    val df = sparkSession.createDataFrame(Seq(Row1("myString"), Row1("foo"), Row1(null))).toDF("foo")
    val records = CAPSRecords.wrap(df)

    // When
    print(records)

    // Then
    val result = getString
    result should equal(
      """!+------------+
         !| foo        |
         !+------------+
         !| 'myString' |
         !| 'foo'      |
         !| null       |
         !+------------+
         !(3 rows)
         !""".stripMargin('!')
    )
  }

  it("prints three columns with three rows") {
    // Given
    val df = sparkSession.createDataFrame(Seq(
      Row3("myString", 4L, false),
      Row3("foo", 99999999L, true),
      Row3(null, -1L, true)
    )).toDF("foo", "v", "veryLongColumnNameWithBoolean")
    val records = CAPSRecords.wrap(df)

    // When
    print(records)

    // Then
    getString should equal(
      """!+-------------------------------------------------------+
         !| foo        | v        | veryLongColumnNameWithBoolean |
         !+-------------------------------------------------------+
         !| 'myString' | 4        | false                         |
         !| 'foo'      | 99999999 | true                          |
         !| null       | -1       | true                          |
         !+-------------------------------------------------------+
         !(3 rows)
         !""".stripMargin('!')
    )
  }

  it("prints return property values without alias") {
    val given =
      initGraph(
        """
          |CREATE (a:Person {name: "Alice"})-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person {name: "Bob"})
        """.stripMargin)

    val when = given.cypher(
      """MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person)
        |RETURN a.name, b.name
        |ORDER BY a.name
      """.stripMargin)

    print(when.getRecords)

    getString should equal(
      """!+-------------------+
         !| a.name  | b.name  |
         !+-------------------+
         !| 'Alice' | 'Bob'   |
         !| 'Bob'   | 'Alice' |
         !+-------------------+
         !(2 rows)
         !""".stripMargin('!'))
  }

  var baos: ByteArrayOutputStream = _

  override def beforeEach(): Unit = {
    baos = new ByteArrayOutputStream()
  }

  private case class Row1(foo: String)

  private case class Row3(foo: String, v: Long, veryLongColumnNameWithBoolean: Boolean)

  private def headerOf(fields: Symbol*): RecordHeader = {
    RecordHeader.from(fields.map(f => Var(f.name)(CTNode)))
  }

  private def getString =
    new String(baos.toByteArray, UTF_8)

  private def print(r: CypherRecords)(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(r)(options.stream(new PrintStream(baos, true, UTF_8.name())))
}
