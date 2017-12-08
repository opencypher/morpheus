/*
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
package org.opencypher.caps.test.support.testgraph

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.harness.TestServerBuilders
import org.opencypher.caps.api.spark.CAPSSession
import org.opencypher.caps.test.support.testgraph.Neo4jTestGraph._

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

final case class Neo4jTestGraph(query: String)(implicit caps: CAPSSession)
    extends TestGraph[GraphDatabaseService, org.neo4j.graphdb.Node, org.neo4j.graphdb.Relationship] {

  private val neo4jServer = TestServerBuilders.newInProcessBuilder()
      .withConfig("dbms.security.auth_enabled", "true")
      .withFixture(query)
      .newServer()

  override val inputGraph: GraphDatabaseService = neo4jServer.graph()

  def close: Any = neo4jServer.close()
}

object Neo4jTestGraph {

  implicit class NeoInputNode(neoNode:org.neo4j.graphdb.Node) extends RichInputNode {
    override def labels: Set[String] = neoNode.getLabels.asScala.map(_.name).toSet

    override def id: Long = neoNode.getId

    override def properties: Map[String, AnyRef] = neoNode.getAllProperties.asScala.toMap
  }

  implicit class NeoInputRelationship(neoRel: org.neo4j.graphdb.Relationship) extends RichInputRelationship {
    override def relType: String = neoRel.getType.name

    override def sourceId: Long = neoRel.getStartNodeId

    override def targetId: Long = neoRel.getEndNodeId

    override def id: Long = neoRel.getId

    override def properties: Map[String, AnyRef] = neoRel.getAllProperties.asScala.toMap
  }

  implicit class NeoInputGraph(neoGraph: GraphDatabaseService)
      extends RichInputGraph[org.neo4j.graphdb.Node, org.neo4j.graphdb.Relationship] {

    override def allNodes: Set[org.neo4j.graphdb.Node] = {
      val tx = neoGraph.beginTx
      val nodes: Set[org.neo4j.graphdb.Node] = neoGraph.getAllNodes.asScala.toSet
      tx.success()
      nodes
    }

    override def allRels: Set[org.neo4j.graphdb.Relationship] = {
      val tx = neoGraph.beginTx
      val rels: Set[org.neo4j.graphdb.Relationship] = neoGraph.getAllRelationships.asScala.toSet
      tx.success()
      rels
    }
  }
}
