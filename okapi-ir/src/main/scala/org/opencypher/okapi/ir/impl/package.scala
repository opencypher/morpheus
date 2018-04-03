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
package org.opencypher.okapi.ir

import cats.data.State
import org.atnos.eff._
import org.atnos.eff.all._
import org.atnos.eff.syntax.all._
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Expr

package object impl {

  type _mayFail[R] = MayFail |= R
  type _hasContext[R] = HasContext |= R

  type MayFail[A] = Either[IRBuilderError, A]
  type HasContext[A] = State[IRBuilderContext, A]

  type IRBuilderStack[A] = Fx.fx2[MayFail, HasContext]

  implicit final class RichIRBuilderStack[A](val program: Eff[IRBuilderStack[A], A]) {

    def run(context: IRBuilderContext): Either[IRBuilderError, (A, IRBuilderContext)] = {
      val stateRun = program.runState(context)
      val errorRun = stateRun.runEither[IRBuilderError]
      errorRun.run
    }
  }

  def error[R: _mayFail : _hasContext, A](err: IRBuilderError)(v: A): Eff[R, A] =
    left[R, IRBuilderError, BlockRegistry[Expr]](err) >> pure(v)

  implicit final class RichSchema(schema: Schema) {

    def forEntityType(cypherType: CypherType): Schema = cypherType match {
      case CTNode(labels, _) =>
        schema.forNode(labels)
      case r: CTRelationship =>
        schema.forRelationship(r)
      case x => throw IllegalArgumentException("entity type", x)
    }

    def addLabelsToCombo(labels: Set[String], combo: Set[String]): Schema = {
      val labelsWithAddition = combo ++ labels
      schema
        .dropPropertiesFor(combo)
        .withNodePropertyKeys(labelsWithAddition, schema.nodeKeys(combo))
    }

    def addPropertyToEntity(propertyKey: String, propertyType: CypherType, entityType: CypherType): Schema = {
      entityType match {
        case CTNode(labels, _) =>
          val allRelevantLabelCombinations = schema.combinationsFor(labels)
          val property = if (allRelevantLabelCombinations.size == 1) propertyType else propertyType.nullable
          allRelevantLabelCombinations.foldLeft(schema) { case (innerCurrentSchema, combo) =>
            val updatedPropertyKeys = innerCurrentSchema.keysFor(Set(combo)).updated(propertyKey, property)
            innerCurrentSchema.withOverwrittenNodePropertyKeys(combo, updatedPropertyKeys)
          }
        case CTRelationship(types, _) =>
          val typesToUpdate = if (types.isEmpty) schema.relationshipTypes else types
          typesToUpdate.foldLeft(schema) { case (innerCurrentSchema, relType) =>
            val updatedPropertyKeys = innerCurrentSchema.relationshipKeys(relType).updated(propertyKey, propertyType)
            innerCurrentSchema.withOverwrittenRelationshipPropertyKeys(relType, updatedPropertyKeys)
          }
        case other => throw IllegalArgumentException("node or relationship to set a property on", other)
      }
    }

  }

}
