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
 */
package org.opencypher.okapi.logical.impl

import org.opencypher.okapi.api.types.{CTBoolean, CTRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.block.Block
import org.opencypher.okapi.ir.api.expr.{Expr, HasType, Ors}
import org.opencypher.okapi.ir.api.pattern.Pattern
import org.opencypher.okapi.ir.api.{IRField, IRGraph, IRNamedGraph, RelType}
import org.opencypher.okapi.ir.impl.util.VarConverters.toVar

case class SolvedQueryModel(
  fields: Set[IRField],
  predicates: Set[Expr] = Set.empty[Expr],
  graphs: Set[IRNamedGraph] = Set.empty[IRNamedGraph]
) {

  // extension
  def withField(f: IRField): SolvedQueryModel = copy(fields = fields + f)
  def withFields(fs: IRField*): SolvedQueryModel = copy(fields = fields ++ fs)
  def withPredicate(pred: Expr): SolvedQueryModel = copy(predicates = predicates + pred)
  def withPredicates(preds: Expr*): SolvedQueryModel = copy(predicates = predicates ++ preds)
  def withGraph(graph: IRNamedGraph): SolvedQueryModel = copy(graphs = graphs + graph)

  def ++(other: SolvedQueryModel): SolvedQueryModel =
    copy(fields ++ other.fields, predicates ++ other.predicates)

  // containment
  def contains(blocks: Block[Expr]*): Boolean = contains(blocks.toSet)
  def contains(blocks: Set[Block[Expr]]): Boolean = blocks.forall(contains)
  def contains(block: Block[Expr]): Boolean = {
    val bindsFields = block.binds.fields subsetOf fields
    val bindsGraphs = block.binds.graphs.map(_.toNamedGraph) subsetOf graphs
    val preds = block.where subsetOf predicates

    bindsFields && bindsGraphs && preds
  }

  def solves(f: IRField): Boolean = fields(f)
  def solves(p: Pattern[Expr]): Boolean = p.fields.subsetOf(fields)
  def solves(p: IRGraph): Boolean = graphs.contains(p.toNamedGraph)

  def solveRelationship(r: IRField): SolvedQueryModel = {
    r.cypherType match {
      case CTRelationship(types) if types.isEmpty =>
        withField(r)
      case CTRelationship(types) =>
        val predicate =
          if (types.size == 1)
            HasType(r, RelType(types.head))(CTBoolean)
          else
            Ors(types.map(t => HasType(r, RelType(t))(CTBoolean)).toSeq: _*)
        withField(r).withPredicate(predicate)
      case _ =>
        throw IllegalArgumentException("a relationship variable", r)
    }
  }
}

object SolvedQueryModel {
  def empty: SolvedQueryModel = SolvedQueryModel(Set.empty, Set.empty, Set.empty)
}
