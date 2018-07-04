/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.neo4j.io

import org.neo4j.driver.v1.Record
import org.neo4j.driver.v1.types.Node
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherNode}

import scala.collection.JavaConverters._

case class Neo4jNode(
  id: Long,
  labels: Set[String] = Set.empty,
  properties: CypherMap = CypherMap.empty)
  extends CypherNode[Long] {
  override type I = Neo4jNode

  override def copy(id: Long, labels: Set[String], properties: CypherMap): Neo4jNode = {
    Neo4jNode(id, labels, properties)
  }
}

object Neo4jIO {

  val defaultIdPropertyName = "id"

  def writeNodesBaseline[T](nodes: Iterator[CypherNode[T]], idProperty: String = defaultIdPropertyName)(implicit neo4j: Neo4jConfig): Unit = {
    neo4j.execute { session =>
      nodes.foreach { node =>
        val labels = node.labels
        val id = node.id

        val nodeLabels = if (labels.isEmpty) ""
        else labels.mkString(":`", "`:`", "`")

        val props = if (node.properties.isEmpty) ""
        else node.properties.toCypherString

        val createQ =
          s"""
             |CREATE (n$nodeLabels $props)
             |SET n.$idProperty = $id
           """.stripMargin
        session.run(createQ).consume()
      }
    }
  }

  def readNodesBaseline(labels: Set[String])
    (implicit neo4j: Neo4jConfig): Iterator[CypherNode[Long]] = {
    neo4j.execute { session =>
      val readQ =
        s"""|MATCH (n)
            |WHERE labels(n) = ${labels.map(l => s"'$l'").mkString("[", ", ", "]")}
            |RETURN n""".stripMargin
      val result = session.run(readQ)
      val rows = result.asScala.map { row: Record =>
        val node: Node = row.get(0).asNode()
        // TODO: This is terribly inefficient. Optimize.
        Neo4jNode(node.id(), labels, CypherMap(node.asMap().asScala.toMap))
      }
      rows
    }
  }

}
