package org.opencypher.spark.prototype

import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.yarn.proto.YarnProtos.QueueInfoProto
import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.opencypher.spark.prototype.ir.{Field, QueryDescriptor, QueryInfo, QueryModel}
import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.block.{Where => IrWhere}
import org.opencypher.spark.prototype.ir.pattern.Pattern
import org.opencypher.spark.prototype.ir.token.TokenRegistry

import scala.collection.immutable.SortedSet

object QueryReprBuilder {
  def from(s: Statement, q: String, tokenDefs: TokenRegistry, params: Set[String]): QueryDescriptor[Expr] = {
    val builder = new QueryReprBuilder(q, tokenDefs, params)
    val blocks = s match {
      case Query(_, part) => part match {
        case SingleQuery(clauses) => clauses.foldLeft(BlockRegistry.empty[Expr]) {
          case (reg, c) => builder.add(c, reg)
        }
      }
      case _ => ???
    }

    builder.build(blocks)
  }
}

object BlockRegistry {
  def empty[E] = BlockRegistry[E](Seq.empty)
}

case class BlockRegistry[E](reg: Seq[(BlockRef, Block[E])]) {

  def register(blockDef: Block[E]): (BlockRef, BlockRegistry[E]) = {
    val ref = BlockRef(generateName(blockDef.blockType))
    ref -> copy(reg = reg :+ ref -> blockDef)
  }

  val c = new AtomicLong()

  private def generateName(t: BlockType) = s"${t.name}_${c.incrementAndGet()}"
}

class QueryReprBuilder(query: String, tokenDefs: TokenRegistry, paramNames: Set[String]) {
  val exprConverter = new ExpressionConverter(tokenDefs)
  val patternConverter = new PatternConverter(tokenDefs)

  var firstBlock: Option[BlockRef] = None

  def add(c: Clause, blockRegistry: BlockRegistry[Expr]): BlockRegistry[Expr] = {
    c match {
      case Match(_, pattern, _, astWhere) =>
        val given = convertPattern(pattern)
        val where = convertWhere(astWhere)

        val after = blockRegistry.reg.headOption.map(_._1).toSet
        val over = BlockSig(Set.empty, Set.empty)
        val block = MatchBlock[Expr](after, over, given, where)
        val (ref, reg) = blockRegistry.register(block)
        reg

      case With(_, _, _, _, _, _) =>
        throw new IllegalArgumentException("With")

      case Return(_, ReturnItems(_, items), _, _, _, _) =>
        val yields = ProjectedFields[Expr](items.map(i => Field(i.name) -> convertExpr(i.expression)).toMap)

        val after = blockRegistry.reg.headOption.map(_._1).toSet
        val projSig = BlockSig(Set.empty, Set.empty)
        val projs = ProjectBlock[Expr](after = after, over = projSig, where = IrWhere.everything, binds = yields)

        val (ref, reg) = blockRegistry.register(projs)
        // TODO: Add rewriter and put the above in case With(...)

        val returnSig = BlockSig(Set.empty, items.map(extract).toSet)

        val returns = SelectBlock[Expr](Set(ref), returnSig, SelectedFields(items.map(extract).toSet))

        val (_, reg2) = reg.register(returns)
        reg2
    }
  }

  private def convertPattern(p: ast.Pattern): Pattern[Expr] = patternConverter.convert(p)
  private def convertExpr(e: ast.Expression): Expr = exprConverter.convert(e)

  private def convertWhere(where: Option[ast.Where]): IrWhere[Expr] = where match {
    case Some(ast.Where(expr)) => convertExpr(expr) match {
      case Ands(exprs) => IrWhere(exprs)
      case e => IrWhere(Set(e))
    }
    case None => IrWhere.everything
  }

  private def extract(r: ReturnItem) = {
    r match {
      case AliasedReturnItem(expr, variable) => Field(variable.name)
      case UnaliasedReturnItem(expr, text) => Field(text)
    }
  }

  def build(blocks: BlockRegistry[Expr]): QueryDescriptor[Expr] = {

    val model = QueryModel(blocks.reg.head._1, tokenDefs, blocks.reg.toMap)

    val maybeReturn = model.blocks.values.find {
      case _: SelectBlock[_] => true
      case _ => false
    }

    val returns = maybeReturn match {
      case None => Set.empty[Field]
      case Some(block) => block.over.outputs
    }
    val userOut = SortedSet(returns.map(f => f -> f.name).toSeq:_*)(fieldOrdering)

    val info = QueryInfo(query)

    QueryDescriptor(info, model, null)
  }
}

object fieldOrdering extends Ordering[(Field, String)] {
  override def compare(x: (Field, String), y: (Field, String)): Int = 0
}

object exprOrdering extends Ordering[(Expr, String)] {
  override def compare(x: (Expr, String), y: (Expr, String)): Int = 0
}
