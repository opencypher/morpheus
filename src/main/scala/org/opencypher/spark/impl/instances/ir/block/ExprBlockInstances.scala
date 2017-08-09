/**
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
package org.opencypher.spark.impl.instances.ir.block

import org.opencypher.spark.api.expr.{Expr, HasLabel, HasType, Var}
import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.block.MatchBlock
import org.opencypher.spark.api.ir.global.{GlobalsRegistry, Label}
import org.opencypher.spark.api.types.{CTNode, CTRelationship}
import org.opencypher.spark.impl.classes.TypedBlock

trait ExprBlockInstances {

  private implicit class RichField(f: Field) {
    def representsNode(v: Var): Boolean =
      f.name == v.name && f.cypherType.subTypeOf(CTNode).isTrue
    def representsRel(v: Var): Boolean =
      f.name == v.name && f.cypherType.subTypeOf(CTRelationship).isTrue
    def withLabel(l: Label): Field = {
      f.copy()(f.cypherType.meet(CTNode(l.name)))
    }
  }

  implicit def typedMatchBlock(implicit globals: GlobalsRegistry) = new TypedBlock[MatchBlock[Expr]] {

    override type BlockExpr = Expr

    override def outputs(block: MatchBlock[Expr]): Set[Field] = {
      val opaqueTypedFields = block.binds.fields
      val predicates = block.where.elements

      predicates.foldLeft(opaqueTypedFields) {
        case (fields, predicate) => predicate match {
          case HasLabel(node: Var, label) => fields.map {
            case f if f representsNode node =>
              f.withLabel(label)
            case f => f
          }
            // The below predicate is never present currently
            // Possibly it will be if we introduce a rewrite
            // Rel types are currently detailed already in pattern conversion
          case HasType(rel: Var, relType) => fields.map {
            case f if f representsRel rel =>
              throw new NotImplementedError("No support for annotating relationships in IR yet")
            case f => f
          }
          case _ => fields
        }
      }
    }
  }
}
