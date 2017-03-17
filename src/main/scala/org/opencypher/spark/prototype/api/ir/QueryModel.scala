package org.opencypher.spark.prototype.api.ir

import org.opencypher.spark.prototype.api.ir.block._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.{DirectedRelationship, EveryNode, EveryRelationship, Pattern}

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom

final case class QueryModel[E](
  result: ResultBlock[E],
  globals: GlobalsRegistry,
//  bindings: Map[ConstantRef, ConstantBinding],
  blocks: Map[BlockRef, Block[E]]
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

  def empty[E](globals: GlobalsRegistry) = QueryModel[E](ResultBlock.empty, globals, Map.empty)

  def base[E](sourceNode: Field, rel: Field, targetNode: Field, globals: GlobalsRegistry): QueryModel[E] = {
    assert(sourceNode != targetNode, "don't do that")

    val ref: BlockRef = BlockRef("match")
    val matchBlock = MatchBlock[E](Set.empty, Pattern.empty
      .withEntity(sourceNode, EveryNode)
      .withEntity(rel, EveryRelationship)
      .withEntity(targetNode, EveryNode)
      .withConnection(rel, DirectedRelationship(sourceNode, targetNode)))
    val blocks: Map[BlockRef, Block[E]] = Map(ref -> matchBlock)

    QueryModel(ResultBlock(Set(ref), FieldsInOrder(sourceNode, rel, targetNode), Set(sourceNode, targetNode), Set(rel)), globals, blocks)
  }

  def nodes[E](node: Field, globals: GlobalsRegistry): QueryModel[E] = {
    val ref: BlockRef = BlockRef("match")
    val matchBlock = MatchBlock[E](Set.empty, Pattern.empty
      .withEntity(node, EveryNode))

    val blocks: Map[BlockRef, Block[E]] = Map(ref -> matchBlock)

    QueryModel(ResultBlock(Set(ref), FieldsInOrder(node), Set(node), Set.empty), globals, blocks)
  }

  def relationships[E](rel: Field, globals: GlobalsRegistry): QueryModel[E] = {
    val ref: BlockRef = BlockRef("match")
    val matchBlock = MatchBlock[E](Set.empty, Pattern.empty
      .withEntity(rel, EveryRelationship))
    val blocks: Map[BlockRef, Block[E]] = Map(ref -> matchBlock)

    QueryModel(ResultBlock(Set(ref), FieldsInOrder(rel), Set.empty, Set(rel)), globals, blocks)
  }
}

case class SolvedQueryModel[E](fields: Set[Field], predicates: Set[E]) {
  def solves(block: Block[E]) = {
    block.binds.fields.subsetOf(fields) && block.where.elts.subsetOf(predicates)
  }
  def withField(f: Field) = copy(fields = fields + f)
  def withFields(fs: Field*) = copy(fields = fields ++ fs)
  def withPredicate(pred: E) = copy(predicates = predicates + pred)
  def contains(blocks: Set[Block[E]]): Boolean = blocks.forall(contains)
  def contains(block: Block[E]): Boolean = {
    val binds = block.binds.fields subsetOf fields
    val preds = block.where.elts subsetOf predicates

    binds && preds
  }
}
object SolvedQueryModel {
  def empty[E] = SolvedQueryModel[E](Set.empty, Set.empty)
}
// sealed trait ConstantBinding
// final case class ParameterBinding(name: String)
