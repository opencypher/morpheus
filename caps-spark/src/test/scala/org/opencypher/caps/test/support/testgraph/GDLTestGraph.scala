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

import org.opencypher.caps.api.spark.CAPSSession
import org.opencypher.caps.demo.Configuration.DefaultType
import org.opencypher.caps.test.support.testgraph.GDLTestGraph._
import org.s1ck.gdl.GDLHandler
import org.s1ck.gdl.model.{Edge, Vertex}

import scala.collection.JavaConverters._

final case class GDLTestGraph(query: String)(implicit caps: CAPSSession)
    extends TestGraph[GDLHandler, Vertex, Edge] {

  override def inputGraph: GDLHandler = new GDLHandler.Builder()
      .disableDefaultVertexLabel()
      .setDefaultEdgeLabel(DefaultType.get())
      .buildFromString(query)
}

object GDLTestGraph {

  implicit class GDLInputNode(node: Vertex) extends RichInputNode {
    override def labels: Set[String] = node.getLabels.asScala.toSet

    override def id: Long = node.getId

    override def properties: Map[String, AnyRef] = node.getProperties.asScala.toMap
  }

  implicit class GDLInputRelationship(rel: Edge) extends RichInputRelationship {
    override def relType: String = rel.getLabel

    override def sourceId: Long = rel.getSourceVertexId

    override def targetId: Long = rel.getTargetVertexId

    override def id: Long = rel.getId

    override def properties: Map[String, AnyRef] = rel.getProperties.asScala.toMap
  }

  implicit class GDLInputGraph(queryHandler: GDLHandler) extends RichInputGraph[Vertex, Edge] {
    override def allNodes: Set[Vertex] = queryHandler.getVertices.asScala.toSet

    override def allRels: Set[Edge] = queryHandler.getEdges.asScala.toSet
  }
}
