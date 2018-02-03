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
package org.opencypher.caps.impl.spark.acceptance
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.test.support.creation.caps.{CAPSGraphFactory, CAPSScanGraphFactory}

import scala.collection.Bag

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSGraphFactory = CAPSScanGraphFactory

  it("Expand into after var expand") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (comment:Comment)
        |CREATE (post1:Post {content: "asdf"})
        |CREATE (post2:Post {content: "foobar"})
        |CREATE (p1)-[:KNOWS]->(p2)
        |CREATE (p2)<-[:HASCREATOR]-(comment)
        |CREATE (comment)-[:REPLYOF]->(post1)-[:REPLYOF]->(post2)
        |CREATE (post2)-[:HASCREATOR]->(p1)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
        |      (p2)<-[e2:HASCREATOR]-(comment:Comment),
        |      (comment)-[e3:REPLYOF*1..10]->(post:Post),
        |      (p1)<-[:HASCREATOR]-(post)
        |WHERE p1.name = "Alice"
        |RETURN p1.name, p2.name, post.content
      """.stripMargin
    )

    // Then
    result.records.toMaps should equal(Bag(
      CypherMap(
        "p1.name" -> "Alice",
        "p2.name" -> "Bob",
        "post.content" -> "foobar"
      )
    ))
  }

}
