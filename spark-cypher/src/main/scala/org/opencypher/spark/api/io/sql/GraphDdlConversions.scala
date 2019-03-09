/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.api.io.sql

import org.opencypher.graphddl.GraphType
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.impl.util.ScalaUtils._

object GraphDdlConversions {

  implicit class SchemaOps(schema: Schema) {
    def asGraphType: GraphType = {
      ???
    }
  }

  implicit class GraphTypeOps(graphType: GraphType) {

    lazy val allPatterns: Set[SchemaPattern] =
      graphType.relTypes.map(edgeType => SchemaPattern(
        sourceLabelCombination = edgeType.startNodeType.labels,
        // TODO: validate there is only one rel type
        relType = edgeType.labels.head,
        targetLabelCombination = edgeType.endNodeType.labels
      ))

    // TODO move out of Graph DDL (maybe to CAPSSchema)
    def asOkapiSchema: Schema = Schema.empty
      .foldLeftOver(graphType.nodeTypes) { case (schema, nodeType) =>
        schema.withNodePropertyKeys(nodeType.labels, graphType.nodePropertyKeys(nodeType))
      }
      .foldLeftOver(graphType.nodeElementTypes) { case (schema, eType) =>
        eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
      }
      // TODO: validate there is only one rel type
      .foldLeftOver(graphType.relTypes) { case (schema, relType) =>
      schema.withRelationshipPropertyKeys(relType.labels.head, graphType.relationshipPropertyKeys(relType))
    }
      // TODO: validate there is only one rel type
      .foldLeftOver(graphType.relElementTypes) { case (schema, eType) =>
      eType.maybeKey.fold(schema)(key => schema.withNodeKey(eType.name, key._2))
    }
      .withSchemaPatterns(allPatterns.toSeq: _*)
  }
}
