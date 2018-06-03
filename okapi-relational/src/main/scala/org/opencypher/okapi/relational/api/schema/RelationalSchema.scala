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
package org.opencypher.okapi.relational.api.schema

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTBoolean, CTNode}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, HasLabel, Property, Var}
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.impl.table.RecordHeaderNew

object RelationalSchema {

  implicit class SchemaOps(val schema: Schema) {

    def headerForNode(node: Var): RecordHeaderNew = {
      val labels: Set[String] = node.cypherType match {
        case CTNode(l, _) => l
        case other     => throw IllegalArgumentException("CTNode", other)
      }
      headerForNode(node, labels)
    }

    def headerForNode(node: Var, labels: Set[String]): RecordHeaderNew = {
      val labelCombos = if (labels.isEmpty) {
        // all nodes scan
        schema.allLabelCombinations
      } else {
        // label scan
        val impliedLabels = schema.impliedLabels.transitiveImplicationsFor(labels)
        schema.combinationsFor(impliedLabels)
      }

      val labelExpressions: Set[Expr] = labelCombos.flatten.map { label =>
        HasLabel(node, Label(label))(CTBoolean)
      }

      val propertyExpressions: Set[Expr] = schema.keysFor(labelCombos).map {
        case (k, t) => Property(node, PropertyKey(k))(t)
      }.toSet

      RecordHeaderNew.empty.withExprs(labelExpressions ++ propertyExpressions + node)
    }
  }

}
