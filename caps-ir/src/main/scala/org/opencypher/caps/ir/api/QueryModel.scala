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
package org.opencypher.caps.ir.api

import java.net.URI

import org.opencypher.caps.api.exception.IllegalStateException
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.ir.api.block._
import org.opencypher.caps.ir.api.pattern._

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom

final case class QueryModel[E](
    result: ResultBlock[E],
    parameters: Map[String, CypherValue],
    blocks: Map[BlockRef, Block[E]],
    graphs: Map[String, URI]
) {

  def apply(ref: BlockRef): Block[E] = blocks(ref)

  def select(fields: Set[IRField]): QueryModel[E] =
    copy(result = result.select(fields))

  def dependencies(ref: BlockRef): Set[BlockRef] = apply(ref).after

  def allDependencies(ref: BlockRef): Set[BlockRef] =
    allDependencies(dependencies(ref).toList, List.empty, Set(ref)) - ref

  @tailrec
  private def allDependencies(
      current: List[BlockRef],
      remaining: List[Set[BlockRef]],
      deps: Set[BlockRef]): Set[BlockRef] = {
    if (current.isEmpty) {
      remaining match {
        case hd :: tl => allDependencies(hd.toList, tl, deps)
        case _        => deps
      }
    } else {
      current match {
        case hd :: _ if deps(hd) =>
          throw IllegalStateException("Cycle of blocks detected!")

        case hd :: tl =>
          allDependencies(tl, dependencies(hd) +: remaining, deps + hd)

        case _ =>
          deps
      }
    }
  }

  def collect[T, That](f: PartialFunction[(BlockRef, Block[E]), T])(
      implicit bf: CanBuildFrom[Map[BlockRef, Block[E]], T, That]): That = {
    blocks.collect(f)
  }
}

case class SolvedQueryModel[E](
    fields: Set[IRField],
    predicates: Set[E] = Set.empty[E],
    graphs: Set[IRNamedGraph] = Set.empty[IRNamedGraph]
) {

  // extension
  def withField(f: IRField): SolvedQueryModel[E] = copy(fields = fields + f)
  def withFields(fs: IRField*): SolvedQueryModel[E] = copy(fields = fields ++ fs)
  def withPredicate(pred: E): SolvedQueryModel[E] = copy(predicates = predicates + pred)
  def withPredicates(preds: E*): SolvedQueryModel[E] = copy(predicates = predicates ++ preds)
  def withGraph(graph: IRNamedGraph): SolvedQueryModel[E] = copy(graphs = graphs + graph)

  def ++(other: SolvedQueryModel[E]): SolvedQueryModel[E] =
    copy(fields ++ other.fields, predicates ++ other.predicates)

  // containment
  def contains(blocks: Block[E]*): Boolean = contains(blocks.toSet)
  def contains(blocks: Set[Block[E]]): Boolean = blocks.forall(contains)
  def contains(block: Block[E]): Boolean = {
    val bindsFields = block.binds.fields subsetOf fields
    val bindsGraphs = block.binds.graphs.map(_.toNamedGraph) subsetOf graphs
    val preds = block.where.elements subsetOf predicates

    bindsFields && bindsGraphs && preds
  }

  def solves(f: IRField): Boolean = fields(f)
  def solves(p: Pattern[E]): Boolean = p.fields.subsetOf(fields)
  def solves(p: IRGraph): Boolean = graphs.contains(p.toNamedGraph)
}

object SolvedQueryModel {
  def empty[E]: SolvedQueryModel[E] = SolvedQueryModel[E](Set.empty, Set.empty, Set.empty)
}
