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
package org.opencypher.okapi.relational.api.schema

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTBoolean, CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.types.CypherTypeUtils._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object RelationalSchema {

  implicit class SchemaOps(val schema: Schema) {

    def headerForElement(element: Var, exactLabelMatch: Boolean = false): RecordHeader = {
      element.cypherType match {
        case _: CTNode => schema.headerForNode(element, exactLabelMatch)
        case _: CTRelationship => schema.headerForRelationship(element)
        case other => throw IllegalArgumentException("Element", other)
      }
    }

    def headerForNode(node: Var, exactLabelMatch: Boolean = false): RecordHeader = {
      val labels: Set[String] = node.cypherType.toCTNode.labels
      headerForNode(node, labels, exactLabelMatch)
    }

    def headerForNode(node: Var, labels: Set[String], exactLabelMatch: Boolean): RecordHeader = {
      val labelCombos = if (exactLabelMatch) {
        Set(labels)
      } else {
        if (labels.isEmpty) {
          // all nodes scan
          schema.allCombinations
        } else {
          // label scan
          val impliedLabels = schema.impliedLabels.transitiveImplicationsFor(labels)
          schema.combinationsFor(impliedLabels)
        }
      }

      val labelExpressions: Set[Expr] = labelCombos.flatten.map { label =>
        HasLabel(node, Label(label))
      }

      val propertyExpressions = schema.nodePropertyKeysForCombinations(labelCombos).map {
        case (k, t) => ElementProperty(node, PropertyKey(k))(t)
      }

      RecordHeader.from(labelExpressions ++ propertyExpressions + node)
    }

    def headerForRelationship(rel: Var): RecordHeader = {
      val types = rel.cypherType match {
        case CTRelationship(relTypes, _) if relTypes.isEmpty =>
          schema.relationshipTypes
        case CTRelationship(relTypes, _) =>
          relTypes
        case other =>
          throw IllegalArgumentException(CTRelationship, other)
      }

      headerForRelationship(rel, types)
    }

    def headerForRelationship(rel: Var, relTypes: Set[String]): RecordHeader = {
      val relKeyHeaderProperties = relTypes
        .flatMap(t => schema.relationshipPropertyKeys(t))
        .groupBy { case (propertyKey, _) => propertyKey }
        .mapValues { keysWithType =>
          keysWithType.toSeq.unzip match {
            case (keys, types) if keys.size == relTypes.size && types.forall(_ == types.head) => types.head
            case (_, types) => types.head.nullable
          }
        }

      val propertyExpressions: Set[Expr] = relKeyHeaderProperties.map {
        case (k, t) => ElementProperty(rel, PropertyKey(k))(t)
      }.toSet

      val startNodeExpr = StartNode(rel)(CTNode)
      val hasTypeExprs = relTypes.map(relType => HasType(rel, RelType(relType)))
      val endNodeExpr = EndNode(rel)(CTNode)

      val relationshipExpressions = hasTypeExprs ++ propertyExpressions + rel + startNodeExpr + endNodeExpr

      RecordHeader.from(relationshipExpressions)
    }
  }

}
