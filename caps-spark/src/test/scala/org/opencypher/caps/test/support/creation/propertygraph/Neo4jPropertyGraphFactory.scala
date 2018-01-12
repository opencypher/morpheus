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
package org.opencypher.caps.test.support.creation.propertygraph

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.harness.TestServerBuilders

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

object Neo4jPropertyGraphFactory extends PropertyGraphFactory{
  lazy val factory = new Neo4jPropertyGraphFactory

  def apply(createQuery: String, parameters: Map[String, Any]): PropertyGraph = factory.create(createQuery, parameters)
}

class Neo4jPropertyGraphFactory {

  private val neo4jServer = TestServerBuilders.newInProcessBuilder()
    .withConfig("dbms.security.auth_enabled", "true")
    .newServer()

  val inputGraph: GraphDatabaseService = neo4jServer.graph()

  def create(createQuery: String, parameters: Map[String, Any]): PropertyGraph = {
    val tx = inputGraph.beginTx()
    inputGraph.execute("MATCH (a) DETACH DELETE a")
    inputGraph.execute(createQuery)

    val nodes = inputGraph.getAllNodes.iterator().asScala.map { neoNode =>
      val labels: Set[String] = neoNode.getLabels.asScala.map(_.name).toSet
      val id: Long = neoNode.getId
      val properties: Map[String, Any] = neoNode.getAllProperties.asScala.toMap

      Node(id, labels, properties)
    }.toSeq

    val relationships = inputGraph.getAllRelationships.iterator().asScala.map { neoRel =>
      val relType: String = neoRel.getType.name
      val sourceId: Long = neoRel.getStartNodeId
      val targetId: Long = neoRel.getEndNodeId
      val id: Long = neoRel.getId
      val properties: Map[String, Any] = neoRel.getAllProperties.asScala.toMap

      Relationship(id, sourceId, targetId, relType, properties)
    }.toSeq

    tx.success()
    tx.close()

    PropertyGraph(nodes, relationships)
  }

  def close: Any = neo4jServer.close()
}
