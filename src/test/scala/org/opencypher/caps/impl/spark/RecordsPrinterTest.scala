/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets.UTF_8

import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.{OpaqueField, RecordHeader}
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.types.CTNode
import org.opencypher.caps.api.util.PrintOptions
import org.opencypher.caps.impl.syntax.header._
import org.opencypher.caps.test.CAPSTestSuite


class RecordsPrinterTest extends CAPSTestSuite {

  implicit val options: PrintOptions = PrintOptions.out

  test("unit table") {
    // Given
    val records = CAPSRecords.unit()

    // When
    print(records)

    // Then
    getString should equal(
      """+----------------------+
        !| (no columns)         |
        !+----------------------+
        !| (no rows)            |
        !+----------------------+
        !""".stripMargin('!')
    )
  }

  test("single column, no rows") {
    // Given
    val records = CAPSRecords.empty(headerOf('foo))

    // When
    print(records)

    // Then
    getString should equal(
      """+----------------------+
        !| foo                  |
        !+----------------------+
        !| (no rows)            |
        !+----------------------+
        !""".stripMargin('!')
    )
  }

  test("single column, three rows") {
    // Given
    val records = CAPSRecords.create(Seq(Row1("myString"), Row1("foo"), Row1(null)))

    // When
    print(records)

    // Then
    getString should equal(
      """+----------------------+
        !| foo                  |
        !+----------------------+
        !| 'myString'           |
        !| 'foo'                |
        !| null                 |
        !+----------------------+
        !""".stripMargin('!')
    )
  }

  test("three columns, three rows") {
    // Given
    val records = CAPSRecords.create(Seq(
      Row3("myString", 4L, false),
      Row3("foo", 99999999L, true),
      Row3(null, -1L, true)
    ))

    // When
    print(records)

    // Then
    getString should equal(
      """+--------------------------------------------------------------------+
        !| foo                  | v                    | veryLongColumnNameWi |
        !+--------------------------------------------------------------------+
        !| 'myString'           | 4                    | false                |
        !| 'foo'                | 99999999             | true                 |
        !| null                 | -1                   | true                 |
        !+--------------------------------------------------------------------+
        !""".stripMargin('!')
    )
  }

  test("return property values without alias") {
    val given = TestGraph(
      """
        |(a:Person {name: "Alice"})-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person {name: "Bob"})
      """.stripMargin)

    val when = given.cypher(
      """MATCH (a:Person)-[:LIVES_IN]->(city:City)<-[:LIVES_IN]-(b:Person)
        |RETURN a.name, b.name
      """.stripMargin)

    print(when.recordsWithDetails)

    getString should equal(
      """+---------------------------------------------+
        !| a.name               | b.name               |
        !+---------------------------------------------+
        !| 'Bob'                | 'Alice'              |
        !| 'Alice'              | 'Bob'                |
        !+---------------------------------------------+
        !""".stripMargin('!'))
  }

  var baos: ByteArrayOutputStream = _

  override def beforeEach(): Unit = {
    baos = new ByteArrayOutputStream()
  }

  private case class Row1(foo: String)
  private case class Row3(foo: String, v: Long, veryLongColumnNameWithBoolean: Boolean)

  private def headerOf(fields: Symbol*): RecordHeader = {
    val value1 = fields.map(f => OpaqueField(Var(f.name)(CTNode)))
    val (header, _) = RecordHeader.empty.update(addContents(value1))
    header
  }

  private def getString =
    new String(baos.toByteArray, UTF_8)

  private def print(r: CAPSRecords)(implicit options: PrintOptions): Unit =
    RecordsPrinter.print(r)(options.stream(new PrintStream(baos, true, UTF_8.name())))
}
