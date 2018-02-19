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
package org.opencypher.caps.impl.spark

import java.net.{URI, URLEncoder}

import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.io.neo4j.Neo4JPropertyGraphDataSourceOld
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.{CAPSSessionFixture, Neo4jServerFixture, SparkSessionFixture, TeamDataFixture}
import org.scalatest.Matchers

class CAPSSessionNeo4jTest
    extends BaseTestSuite
    with SparkSessionFixture
    with CAPSSessionFixture
    with Neo4jServerFixture
    with TeamDataFixture
    with Matchers {

  test("Neo4j via URI") {
    val nodeQuery = URLEncoder.encode("MATCH (n) RETURN n", "UTF-8")
    val relQuery = URLEncoder.encode("MATCH ()-[r]->() RETURN r", "UTF-8")
    val uri = URI.create(s"$neo4jHost?$nodeQuery;$relQuery")

    val graph = caps.readFrom(uri).asCaps

    graph.nodes("n").toDF().collect().toBag should equal(teamDataGraphNodes)
    graph.relationships("rel").toDF().collect().toBag should equal(teamDataGraphRels)
  }

  test("Neo4j via mount point") {
    caps.mount(
      Neo4JPropertyGraphDataSourceOld(neo4jConfig, Some("MATCH (n) RETURN n" -> "MATCH ()-[r]->() RETURN r")),
      "/neo4j1")

    val graph = caps.readFrom("/neo4j1").asCaps
    graph.nodes("n").toDF().collect().toBag should equal(teamDataGraphNodes)
    graph.relationships("rel").toDF().collect().toBag should equal(teamDataGraphRels)
  }
}
