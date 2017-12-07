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

import org.neo4j.graphdb.{GraphDatabaseService, Node, Relationship}
import org.neo4j.harness.TestServerBuilders
import org.opencypher.caps.api.spark.CAPSSession

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

final case class Neo4jTestGraph(query: String)(implicit caps: CAPSSession) extends TestGraph {
  
  private val neo4jServer = TestServerBuilders.newInProcessBuilder()
      .withConfig("dbms.security.auth_enabled", "true")
      .withFixture(query)
      .newServer()

  override val inputGraph = new NeoRichInputGraph(neo4jServer.graph())

  implicit class NeoRichInputNode(neoNode: Node) extends RichInputNode {
    override def getLabels: Set[String] = neoNode.getLabels.asScala.map(_.name).toSet

    override def getId: Long = neoNode.getId

    override def getProperties: Map[String, AnyRef] = neoNode.getAllProperties.asScala.toMap
  }

  implicit class NeoRichInputRelationship(neoRel: Relationship) extends RichInputRelationship {
    override def getType: String = neoRel.getType.name

    override def getSourceId: Long = neoRel.getStartNodeId

    override def getTargetId: Long = neoRel.getEndNodeId

    override def getId: Long = neoRel.getId

    override def getProperties: Map[String, AnyRef] = neoRel.getAllProperties.asScala.toMap
  }

  implicit class NeoRichInputGraph(neoGraph: GraphDatabaseService) extends RichInputGraph {
    override def getAllNodes: Set[RichInputNode] = {
      val tx = neoGraph.beginTx
      val nodes: Set[RichInputNode] = neoGraph.getAllNodes.asScala.map(new NeoRichInputNode(_)).toSet
      tx.success()
      nodes
    }

    override def getAllRelationships: Set[RichInputRelationship] = {
      val tx = neoGraph.beginTx
      val rels: Set[RichInputRelationship] = neoGraph.getAllRelationships.asScala.map(new NeoRichInputRelationship(_)).toSet
      tx.success()
      rels
    }
  }
}
