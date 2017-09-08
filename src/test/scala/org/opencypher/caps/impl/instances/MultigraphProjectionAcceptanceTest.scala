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
package org.opencypher.caps.impl.instances

import java.net.URI

import org.opencypher.caps.CAPSTestSuite
import org.opencypher.caps.api.io.{GraphSource, PersistMode}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.api.value.CypherMap

import scala.collection.immutable.Bag

class MultigraphProjectionAcceptanceTest extends CAPSTestSuite {

  ignore("Can select a source graph to match data from") {
    val result = testGraph1.testGraph.graph.cypher("FROM GRAPH myGraph AT '/test/graph2' MATCH (n:Person) RETURN n.name AS name")

    result.records.toMaps should equal(Bag(
      CypherMap("name" -> "Phil")
    ))
  }

  private def testGraph1 = TestGraphSource("/test/graph1", TestGraph("(a:Person {name: 'Mats'})"))
  private def testGraph2 = TestGraphSource("/test/graph2", TestGraph("(a:Person {name: 'Phil'})"))

  override def initCAPSSession: CAPSSession = {
    CAPSSession
      .builder(session)
      .withGraphSource(testGraph1.uriString, testGraph1)
      .withGraphSource(testGraph2.uriString, testGraph2)
      .get
  }

  private case class TestGraphSource(uriString: String, testGraph: TestGraph) extends GraphSource {
    override val canonicalURI: URI = URI.create(uriString)
    private lazy val capsGraph = testGraph.graph
    override def sourceForGraphAt(uri: URI): Boolean = uri == canonicalURI
    override def create(implicit capsSession: CAPSSession): CAPSGraph = ???
    override def graph(implicit capsSession: CAPSSession): CAPSGraph = capsGraph
    override def schema(implicit capsSession: CAPSSession): Option[Schema] = Some(capsGraph.schema)
    override def persist(mode: PersistMode, graph: CAPSGraph)(implicit capsSession: CAPSSession): CAPSGraph = ???
    override def delete(implicit capsSession: CAPSSession): Unit = ???
  }
}
