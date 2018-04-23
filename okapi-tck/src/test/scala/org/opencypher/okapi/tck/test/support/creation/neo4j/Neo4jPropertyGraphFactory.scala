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
package org.opencypher.okapi.tck.test.support.creation.neo4j

import java.util.stream.Collectors

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.harness.TestServerBuilders
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.test.support.creation.propertygraph._
import org.opencypher.okapi.testing.propertygraph
import org.opencypher.okapi.testing.propertygraph.{PropertyGraphFactory, TestNode, TestPropertyGraph, TestRelationship}

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

object Neo4jPropertyGraphFactory extends PropertyGraphFactory {
  lazy val factory = new Neo4jPropertyGraphFactory

  def apply(createQuery: String, parameters: Map[String, Any]): TestPropertyGraph = factory.create(createQuery, parameters)
}

class Neo4jPropertyGraphFactory {

  private val neo4jServer = TestServerBuilders
    .newInProcessBuilder()
    .withConfig("dbms.security.auth_enabled", "true")
    .newServer()

  val inputGraph: GraphDatabaseService = neo4jServer.graph()

  def create(createQuery: String, parameters: Map[String, Any]): TestPropertyGraph = {
    val tx = inputGraph.beginTx()
    inputGraph.execute("MATCH (a) DETACH DELETE a")
    inputGraph.execute(createQuery)

    val propertyGraph = getPropertyGraph

    tx.success()
    tx.close()

    propertyGraph
  }

  private def getPropertyGraph = {
    val neoNodes = inputGraph.getAllNodes.iterator().stream().collect(Collectors.toList())
    val nodes = neoNodes.asScala.map { neoNode =>
      val labels: Set[String] = neoNode.getLabels.asScala.map(_.name).toSet
      val id: Long = neoNode.getId
      val properties = CypherMap(neoNode.getAllProperties.asScala.toSeq: _*)

      TestNode(id, labels, properties)
    }

    val neoRels = inputGraph.getAllRelationships.iterator().stream().collect(Collectors.toList())
    val relationships = neoRels.asScala.map { neoRel =>
      val relType: String = neoRel.getType.name
      val sourceId: Long = neoRel.getStartNodeId
      val targetId: Long = neoRel.getEndNodeId
      val id: Long = neoRel.getId
      val properties = CypherMap(neoRel.getAllProperties.asScala.toSeq: _*)

      TestRelationship(id, sourceId, targetId, relType, properties)
    }

    propertygraph.TestPropertyGraph(nodes, relationships)
  }

  def close: Any = neo4jServer.close()
}
