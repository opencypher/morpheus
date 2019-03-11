/**
  * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
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
package org.opencypher.spark.api.io.sql.util

import org.opencypher.graphddl._
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.fs.DefaultGraphDirectoryStructure.{concatDirectoryNames, nodeTableDirectoryName, relKeyTableDirectoryName}
import org.opencypher.spark.api.io.{GraphEntity, Relationship}

object DdlUtils {
  implicit class PropertyGraphOps(pg: PropertyGraph) {

    def defaultDdl(
      graphName: GraphName,
      graphType: GraphType,
      maybeDataSourceName: Option[String] = None,
      maybeDatabaseName: Option[String] = None,
      maybeSchemaDefinition: Option[SetSchemaDefinition] = None
    )(implicit caps: CAPSSession): GraphDdl = {
      val schema = graphType
      val pathPrefixParts: List[String] = List(maybeDataSourceName, maybeDatabaseName).flatten
      val joinFromStartNode: List[Join] = List(Join(GraphEntity.sourceIdKey, Relationship.sourceStartNodeKey))
      val joinFromEndNode: List[Join] = List(Join(GraphEntity.sourceIdKey, Relationship.sourceEndNodeKey))

      def nodeViewId(labelCombination: Set[String]): ViewId = {
        ViewId(maybeSchemaDefinition, pathPrefixParts :+ nodeTableDirectoryName(labelCombination))
      }

      def relViewId(sourceLabelCombination: Set[String], relType: String, targetLabelCombination: Set[String]): ViewId = {
        val relTypeDir = concatDirectoryNames(Seq(
          nodeTableDirectoryName(sourceLabelCombination),
          relKeyTableDirectoryName(relType),
          nodeTableDirectoryName(targetLabelCombination)))
        ViewId(maybeSchemaDefinition, pathPrefixParts :+ relTypeDir)
      }

      def nodeViewKey(labelCombination: Set[String]): NodeViewKey = {
        NodeViewKey(NodeType(labelCombination), nodeViewId(labelCombination))
      }

      val nodeToViewMappings: Map[NodeViewKey, NodeToViewMapping] = {
        schema.nodeTypes.map { nodeType =>
          NodeToViewMapping(
            nodeType,
            nodeViewId(nodeType.labels),
            schema.nodePropertyKeys(nodeType).keySet.map(k => k -> k).toMap)
        }.map(mapping => mapping.key -> mapping).toMap
      }

      val edgeToViewMappings: List[EdgeToViewMapping] = schema.relTypes.map { relType =>
        val relKeyMapping = schema.relationshipPropertyKeys(relType).keySet.map(k => k -> k).toMap
        EdgeToViewMapping(
          relType,
          relViewId(relType.startNodeType.labels, relType.labels.head, relType.endNodeType.labels),
          StartNode(nodeViewKey(relType.startNodeType.labels), joinFromStartNode),
          EndNode(nodeViewKey(relType.endNodeType.labels), joinFromEndNode),
          relKeyMapping)
      }.toList

      GraphDdl(Map(graphName -> Graph(
        graphName,
        graphType,
        nodeToViewMappings,
        edgeToViewMappings
      )))
    }
  }
}
