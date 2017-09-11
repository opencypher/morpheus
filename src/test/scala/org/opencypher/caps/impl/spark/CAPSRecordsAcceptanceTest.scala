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

import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.impl.spark.io.neo4j.Neo4jGraphLoader
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.{Neo4jServerFixture, OpenCypherDataFixture}

import scala.language.reflectiveCalls

class CAPSRecordsAcceptanceTest extends CAPSTestSuite with Neo4jServerFixture with OpenCypherDataFixture {

  private lazy val graph: CAPSGraph =
    Neo4jGraphLoader.fromNeo4j(neo4jConfig)

  test("label scan and project") {
    // When
    val result = graph.cypher("MATCH (a:Person) RETURN a.name")

    // Then
    result.records shouldHaveSize 15 andContain "Rachel Kempson"
  }

  test("expand and project") {
    // When
    val result = graph.cypher("MATCH (a:Actor)-[r]->(m:Film) RETURN a.birthyear, m.title")

    // Then
    result.records shouldHaveSize 8 andContain 1952 -> "Batman Begins"
  }

  test("filter rels on property") {
    // Given
    val query = "MATCH (a:Actor)-[r:ACTED_IN]->() WHERE r.charactername = 'Guenevere' RETURN a, r"

    // When
    val result = graph.cypher(query)

    // Then
    result.records shouldHaveSize 1
  }

  test("filter nodes on property") {
    // When
    val result = graph.cypher("MATCH (p:Person) WHERE p.birthyear = 1970 RETURN p.name")

    // Then
    result.records shouldHaveSize 3 andContain "Christopher Nolan"
  }

  test("expand and project, three properties") {
    // Given
    val query = "MATCH (a:Actor)-[:ACTED_IN]->(f:Film) RETURN a.name, f.title, a.birthyear"

    // When
    val result = graph.cypher(query)

    // Then
    val tuple = (
      "Natasha Richardson",
      "The Parent Trap",
      1963
    )
    result.records shouldHaveSize 8 andContain tuple
  }

  test("handle properties with same key and different type between labels") {
    // When
    val movieResult = graph.cypher("MATCH (m:Movie) RETURN m.title")

    // Then
    movieResult.records shouldHaveSize 1 andContain 444

    // When
    val filmResult = graph.cypher("MATCH (f:Film) RETURN f.title")

    // Then
    filmResult.records shouldHaveSize 5 andContain "Camelot"
  }

  test("multiple hops of expand with different reltypes") {
    // Given
    val query = "MATCH (c:City)<-[:BORN_IN]-(a:Actor)-[r:ACTED_IN]->(f:Film) RETURN a.name, c.name, f.title"

    // When
    val records = graph.cypher(query).records

    // Then
    val tuple = (
      "Lindsay Lohan",
      "New York",
      "The Parent Trap"
    )
    records shouldHaveSize 4 andContain tuple
  }

  // TODO: Figure out what invariant this was meant to measure
  ignore("multiple hops of expand with possible reltype conflict") {
    // Given
    val query = "MATCH (u1:User)-[r1:POSTED]->(t:Tweet)-[r2]->(u2:User) RETURN u1.name, u2.name, t.text"

    // When
    val result = graph.cypher(query)

    // Then
    val tuple = ("Brendan Madden", "Tom Sawyer Software",
      "#tsperspectives 7.6 is 15% faster with #neo4j Bolt support. https://t.co/1xPxB9slrB @TSawyerSoftware #graphviz")
    result.records shouldHaveSize 79 andContain tuple
  }

  // TODO: Pull out/move over to GraphMatching
  implicit class OtherRichRecords(records: CAPSRecords) {
    def shouldHaveSize(size: Int) = {
      val tuples = records.data.collect().toSeq.map { r =>
        val cells = records.header.slots.map { s =>
          r.get(s.index)
        }

        asProduct(cells)
      }

      tuples.size shouldBe size

      new {
        def andContain(contents: Product): Unit = {
          tuples should contain(contents)
        }

        def andContain(contents: Any): Unit = andContain(Tuple1(contents))
      }
    }

    def asProduct(elts: IndexedSeq[Any]): Product = elts.length match {
      case 0 => throw new IllegalArgumentException("Can't turn empty sequence into a tuple")
      case 1 => Tuple1(elts(0))
      case 2 => Tuple2(elts(0), elts(1))
      case 3 => Tuple3(elts(0), elts(1), elts(2))
      case 4 => Tuple4(elts(0), elts(1), elts(2), elts(3))
      case 5 => Tuple5(elts(0), elts(1), elts(2), elts(3), elts(4))
      case 6 => Tuple6(elts(0), elts(1), elts(2), elts(3), elts(4), elts(5))
      case 7 => Tuple7(elts(0), elts(1), elts(2), elts(3), elts(4), elts(5), elts(6))
      case 8 => Tuple8(elts(0), elts(1), elts(2), elts(3), elts(4), elts(5), elts(6), elts(7))
      case 9 => Tuple9(elts(0), elts(1), elts(2), elts(3), elts(4), elts(5), elts(6), elts(7), elts(8))
      case _ => throw new UnsupportedOperationException("Implement support for larger products")
    }
  }
}
