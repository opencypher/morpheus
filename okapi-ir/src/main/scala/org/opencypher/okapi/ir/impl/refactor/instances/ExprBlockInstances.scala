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
package org.opencypher.okapi.ir.impl.refactor.instances

import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.block.MatchBlock
import org.opencypher.okapi.ir.api.expr.{Expr, HasLabel, HasType, Var}
import org.opencypher.okapi.ir.api.{IRField, Label}
import org.opencypher.okapi.ir.impl.refactor.syntax.TypedBlock

trait ExprBlockInstances {

  private implicit class RichIRField(f: IRField) {
    def representsNode(v: Var): Boolean =
      f.name == v.name && f.cypherType.subTypeOf(CTNode).isTrue
    def representsRel(v: Var): Boolean =
      f.name == v.name && f.cypherType.subTypeOf(CTRelationship).isTrue
    def withLabel(l: Label): IRField = {
      f.copy()(cypherType = f.cypherType.meet(CTNode(Set(l.name), f.cypherType.graph)))
    }
  }

  implicit def typedMatchBlock: TypedBlock[MatchBlock] =
    new TypedBlock[MatchBlock] {

      override type BlockExpr = Expr

      override def outputs(block: MatchBlock): Set[IRField] = {
        val opaqueTypedFields = block.binds.fields
        val predicates = block.where

        predicates.foldLeft(opaqueTypedFields) {
          case (fields, predicate) =>
            predicate match {
              case HasLabel(node: Var, label) =>
                fields.map {
                  case f if f representsNode node =>
                    f.withLabel(label)
                  case f => f
                }
              // The below predicate is never present currently
              // Possibly it will be if we introduce a rewrite
              // Rel types are currently detailed already in pattern conversion
              case HasType(rel: Var, _) =>
                fields.map {
                  case f if f representsRel rel =>
                    throw NotImplementedException("No support for annotating relationships in IR yet")
                  case f => f
                }
              case _ => fields
            }
        }
      }
    }
}
