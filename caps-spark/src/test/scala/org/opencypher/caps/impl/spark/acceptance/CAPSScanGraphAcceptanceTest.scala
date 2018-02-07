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
import java.net.URI

import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.demo.Configuration.PrintLogicalPlan
import org.opencypher.caps.impl.spark.io.file.FileCsvPropertyGraphDataSource
import org.opencypher.caps.test.support.creation.caps.{CAPSGraphFactory, CAPSScanGraphFactory}

import scala.collection.immutable.Bag
import org.opencypher.caps.impl.spark.CAPSConverters._

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSGraphFactory = CAPSScanGraphFactory
  it("can convert ListLiterals with nested non literal expressions") {
    val graph = initGraph("CREATE (:A{val: 1}), (:A{val: 2})")

    val result = graph.cypher(
      """
        |MATCH (n:A)
        |WITH [n.val*10, n.val*100] as vals
        |RETURN vals""".stripMargin)

    result.records.toMaps should equal(Bag(
      CypherMap("vals" -> Seq(10, 100)),
      CypherMap("vals" -> Seq(20, 200))
    ))
  }

  it("bar") {
    val graph = FileCsvPropertyGraphDataSource(URI.create("/home/max/coding/neo/caps-benchmarks/src/test/resources/validation_db")).graph
    val result = graph.cypher(
      """
        |MATCH (countryX:Country {name:{countryXName}}),
        |      (countryY:Country {name:{countryYName}}),
        |      (person:Person {id:{personId}})
        |WITH person, countryX, countryY
        |LIMIT 1
        |MATCH (city:City)-[:IS_PART_OF]->(country:Country)
        |WHERE country IN [countryX, countryY]
        |WITH person, countryX, countryY, collect(id(city)) AS cities
        |MATCH (person)-[:KNOWS*1..2]-(friend:Person)-[:PERSON_IS_LOCATED_IN]->(city:City)
        |WHERE NOT person=friend AND NOT id(city) IN cities
        |WITH DISTINCT friend, countryX, countryY
        |MATCH (friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message:Message),
        |      (message)-[:POST_IS_LOCATED_IN|COMMENT_IS_LOCATED_IN]->(country:Country)
        |WHERE {maxDate} > message.creationDate >= {startDate}
        |  AND id(country) IN [id(countryX), id(countryY)]
        |WITH friend,
        |     CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
        |     CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
        |WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
        |WHERE xCount>0 AND yCount>0
        |RETURN friend.id AS friendId,
        |       friend.firstName AS friendFirstName,
        |       friend.lastName AS friendLastName,
        |       xCount,
        |       yCount,
        |       xCount + yCount AS xyCount
        |ORDER BY xyCount DESC, friendId ASC
        |LIMIT {limit}
      """.stripMargin, Map(
        "personId" -> CypherValue(3298534886136L),
        "countryXName" -> CypherValue("Mongolia"),
        "maxDate" -> CypherValue(1280880000000L),
        "countryYName" -> CypherValue("Lithuania"),
        "durationDays" -> CypherValue(34),
        "limit" -> CypherValue(20),
        "startDate" -> CypherValue(1277942400000L))
    )

    result.records.print
  }
}
