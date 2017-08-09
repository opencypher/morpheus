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
package org.opencypher.spark.api.ir

import org.opencypher.spark.api.types.{CTNode, CTRelationship}
import org.opencypher.spark.api.ir.block._
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.ir.pattern._
import org.opencypher.spark.api.schema.Schema

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom

final case class QueryModel[E](
  result: ResultBlock[E],
  globals: GlobalsRegistry,
//  bindings: Map[ConstantRef, ConstantBinding],
  blocks: Map[BlockRef, Block[E]],
  schemas: Map[BlockRef, Schema]
) {

  def apply(ref: BlockRef): Block[E] = blocks(ref)

  def select(fields: Set[Field]) =
    copy(result = result.select(fields))

  def dependencies(ref: BlockRef): Set[BlockRef] = apply(ref).after

  def allDependencies(ref: BlockRef): Set[BlockRef] =
    allDependencies(dependencies(ref).toList, List.empty, Set(ref)) - ref

  @tailrec
  private def allDependencies(current: List[BlockRef], remaining: List[Set[BlockRef]], deps: Set[BlockRef])
  : Set[BlockRef] = {
    if (current.isEmpty) {
      remaining match {
        case hd :: tl => allDependencies(hd.toList, tl, deps)
        case _ => deps
      }
    } else {
      current match {
        case hd :: _ if deps(hd) =>
          throw new IllegalStateException("Cycle of blocks detected!")

        case hd :: tl =>
          allDependencies(tl, dependencies(hd) +: remaining, deps + hd)

        case _ =>
          deps
      }
    }
  }

  def collect[T, That](f: PartialFunction[(BlockRef, Block[E]), T])(implicit bf: CanBuildFrom[Map[BlockRef, Block[E]], T, That]): That = {
    blocks.collect(f)
  }
}

object QueryModel {

  def empty[E](globals: GlobalsRegistry) = {
    // TODO: empty graph?
    val graphBlock = LoadGraphBlock[E](Set.empty, DefaultGraph())
    val ref = BlockRef("graph")
    QueryModel[E](ResultBlock.empty(ref), globals, Map(ref -> graphBlock), Map(ref -> Schema.empty))
  }

  def base[E](sourceNodeName: String, relName: String, targetNodeName: String, globals: GlobalsRegistry): QueryModel[E] = {
    val sourceNode = Field(sourceNodeName)(CTNode)
    val rel = Field(relName)(CTRelationship)
    val targetNode = Field(targetNodeName)(CTNode)

    assert(sourceNode != targetNode, "don't do that")

    val graphBlockRef = BlockRef("graph")
    val graphBlock = LoadGraphBlock[E](Set.empty, DefaultGraph())

    val ref: BlockRef = BlockRef("match")
    val matchBlock = MatchBlock[E](Set.empty, Pattern.empty
      .withEntity(sourceNode, EveryNode)
      .withEntity(rel, EveryRelationship)
      .withEntity(targetNode, EveryNode)
      .withConnection(rel, DirectedRelationship(sourceNode, targetNode)), AllGiven[E](), graphBlockRef)
    val blocks: Map[BlockRef, Block[E]] = Map(ref -> matchBlock)

    val resultBlock = ResultBlock[E](Set(ref), FieldsInOrder(sourceNode, rel, targetNode), Set(sourceNode, targetNode), Set(rel), graphBlockRef)
    QueryModel(resultBlock, globals, blocks, Map(graphBlockRef -> Schema.empty))
  }

  def nodes[E](nodeName: String, globals: GlobalsRegistry): QueryModel[E] = {
    val node = Field(nodeName)(CTNode)

    val graphBlockRef = BlockRef("graph")
    val graphBlock = LoadGraphBlock[E](Set.empty, DefaultGraph())

    val ref: BlockRef = BlockRef("match")
    val matchBlock = MatchBlock[E](Set.empty, Pattern.empty
      .withEntity(node, EveryNode), AllGiven[E](), graphBlockRef)

    val blocks: Map[BlockRef, Block[E]] = Map(ref -> matchBlock)

    val resultBlock = ResultBlock[E](Set(ref), FieldsInOrder(node), Set(node), Set.empty, graphBlockRef)
    QueryModel(resultBlock, globals, blocks, Map(graphBlockRef -> Schema.empty))
  }

  def relationships[E](relName: String, globals: GlobalsRegistry): QueryModel[E] = {
    val rel = Field(relName)(CTRelationship)

    val graphBlockRef = BlockRef("graph")
    val graphBlock = LoadGraphBlock[E](Set.empty, DefaultGraph())

    val ref: BlockRef = BlockRef("match")
    val matchBlock = MatchBlock[E](Set.empty, Pattern.empty
      .withEntity(rel, EveryRelationship), AllGiven[E](), graphBlockRef)
    val blocks: Map[BlockRef, Block[E]] = Map(ref -> matchBlock)

    val resultBlock = ResultBlock[E](Set(ref), FieldsInOrder(rel), Set.empty, Set(rel), graphBlockRef)
    QueryModel(resultBlock, globals, blocks, Map(graphBlockRef -> Schema.empty))
  }
}

case class SolvedQueryModel[E](fields: Set[Field], predicates: Set[E]) {

  // extension
  def withField(f: Field): SolvedQueryModel[E] = copy(fields = fields + f)
  def withFields(fs: Field*): SolvedQueryModel[E] = copy(fields = fields ++ fs)
  def withPredicate(pred: E): SolvedQueryModel[E] = copy(predicates = predicates + pred)

  def ++(other: SolvedQueryModel[E]): SolvedQueryModel[E] =
    copy(fields ++ other.fields, predicates ++ other.predicates)

  // containment
  def contains(blocks: Block[E]*): Boolean = contains(blocks.toSet)
  def contains(blocks: Set[Block[E]]): Boolean = blocks.forall(contains)
  def contains(block: Block[E]): Boolean = {
    val binds = block.binds.fields subsetOf fields
    val preds = block.where.elements subsetOf predicates

    binds && preds
  }

  def solves(f: Field): Boolean = fields(f)
  def solves(p: Pattern[E]): Boolean = p.fields.subsetOf(fields)
}

object SolvedQueryModel {
  def empty[E]: SolvedQueryModel[E] = SolvedQueryModel[E](Set.empty, Set.empty)
}
