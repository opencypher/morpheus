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
package org.opencypher.okapi.relational.api.io

import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.types.{CTBoolean, CTNode}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.impl.table.RecordHeaderNew

object RelationalEntityMapping {

  implicit class EntityMappingOps(val mapping: EntityMapping) {
    def toRecordHeader: RecordHeaderNew = mapping match {
      case n: NodeMapping => headerFrom(n)
      case r: RelationshipMapping => headerFrom(r)
    }

    def id(v: Var): (Var, String) = v -> mapping.sourceIdKey

    def properties(v: Var): Map[Property, String] = mapping.propertyMapping.map {
      // TODO: would be nice to have the CypherType here
      case (key, sourceColumn) => Property(v, PropertyKey(key))() -> sourceColumn
    }
  }

  implicit class NodeMappingOps(val mapping: NodeMapping) {

    def optionalLabels(node: Var): Map[HasLabel, String] = mapping.optionalLabelMapping.map {
      case (label, sourceColumn) => HasLabel(node, Label(label))(CTBoolean) -> sourceColumn
    }
  }

  implicit class RelationshipMappingOps(val mapping: RelationshipMapping) {

    def startNode(rel: Var): (StartNode, String) = StartNode(rel)(CTNode) -> mapping.sourceStartNodeKey

    def endNode(rel: Var): (EndNode, String) = EndNode(rel)(CTNode) -> mapping.sourceEndNodeKey
  }

  def headerFrom(nodeMapping: NodeMapping): RecordHeaderNew = {
    val nodeVar = Var("")(nodeMapping.cypherType)

    val exprToColumn = Map[Expr, String](nodeMapping.id(nodeVar)) ++
      nodeMapping.optionalLabels(nodeVar) ++
      nodeMapping.properties(nodeVar)

    RecordHeaderNew(exprToColumn)
  }

  def headerFrom(relationshipMapping: RelationshipMapping): RecordHeaderNew = {
    val relVar = Var("")(relationshipMapping.cypherType)

    val exprToColumn = Map[Expr, String](
      relationshipMapping.id(relVar),
      relationshipMapping.startNode(relVar),
      relationshipMapping.endNode(relVar)) ++ relationshipMapping.properties(relVar)

    RecordHeaderNew(exprToColumn)
  }
}
