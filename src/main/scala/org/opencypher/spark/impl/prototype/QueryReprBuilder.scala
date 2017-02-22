package org.opencypher.spark.impl.prototype

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.neo4j.cypher.internal.frontend.v3_2.ast._

import scala.collection.immutable.SortedSet

object QueryReprBuilder {
  def from(s: Statement, q: String, tokenDefs: TokenDefs, params: Set[String]): QueryRepresentation[Expr] = {
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

case class BlockRegistry[E](reg: Seq[(BlockRef, BlockDef[E])]) {

  def register(blockDef: BlockDef[E]): (BlockRef, BlockRegistry[E]) = {
    val ref = BlockRef(generateName(blockDef.blockType))
    ref -> copy(reg = reg :+ ref -> blockDef)
  }

  val c = new AtomicLong()

  private def generateName(t: BlockType) = s"${t.name}_${c.incrementAndGet()}"
}

class QueryReprBuilder(query: String, tokenDefs: TokenDefs, paramNames: Set[String]) {
  val exprConverter = new ExpressionConverter(tokenDefs)
  val patternConverter = new PatternConverter(tokenDefs)

  var firstBlock: Option[BlockRef] = None

  def add(c: Clause, blockRegistry: BlockRegistry[Expr]): BlockRegistry[Expr] = {
    c match {
      case Match(_, pattern, _, astWhere) =>
        val given = convertPattern(pattern)
        val where = convertWhere(astWhere)

        val after = blockRegistry.reg.headOption.map(_._1).toSet
        val over = BlockSignature(Set.empty, Set.empty)
        val block = MatchBlock[Expr](after, over, given, where)
        val (ref, reg) = blockRegistry.register(block)
        reg

      case With(_, _, _, _, _, _) =>
        throw new IllegalArgumentException("With")

      case Return(_, ReturnItems(_, items), _, _, _, _) =>
        val yields = Yields(items.map(i => convertExpr(i.expression)).toSet)

        val after = blockRegistry.reg.headOption.map(_._1).toSet
        val projSig = BlockSignature(Set.empty, Set.empty)
        val projs = ProjectBlock(after = after, over = projSig, where = Where.everything, yields = yields)

        val (ref, reg) = blockRegistry.register(projs)
        // TODO: Add rewriter and put the above in case With(...)

        val returnSig = BlockSignature(Set.empty, items.map(extract).toSet)
        val returns = ReturnBlock[Expr](Set(ref), returnSig)

        val (_, reg2) = reg.register(returns)
        reg2
    }
  }

  private def convertPattern(p: ast.Pattern): Given = patternConverter.convert(p)
  private def convertExpr(e: ast.Expression): Expr = exprConverter.convert(e)

  private def convertWhere(where: Option[ast.Where]): Where[Expr] = where match {
    case Some(ast.Where(expr)) => convertExpr(expr) match {
      case Ands(exprs) => Where(exprs)
      case e => Where(Set(e))
    }
    case None => Where.everything
  }

  private def extract(r: ReturnItem) = {
    r match {
      case AliasedReturnItem(expr, variable) => Field(variable.name)
      case UnaliasedReturnItem(expr, text) => Field(text)
    }
  }

  def build(blocks: BlockRegistry[Expr]): QueryRepresentation[Expr] = {

    val blockStructure = BlockStructure(blocks.reg.toMap, blocks.reg.head._1)

    val parameters = paramNames.map(s => ParameterNameGenerator.generate(s) -> s).toMap

    val maybeReturn = blockStructure.blocks.values.find {
      case _: ReturnBlock[_] => true
      case _ => false
    }

    val returns = maybeReturn match {
      case None => Set.empty[Field]
      case Some(block) => block.over.outputs
    }
    val root = RootBlockImpl(returns, parameters.keySet, Set.empty, tokenDefs, blockStructure)

    val userOut = SortedSet(returns.map(f => f -> f.name).toSeq:_*)(fieldOrdering)

    QueryRepr(query, userOut, parameters, root)
  }
}

object fieldOrdering extends Ordering[(Field, String)] {
  override def compare(x: (Field, String), y: (Field, String)): Int = 0
}

object exprOrdering extends Ordering[(Expr, String)] {
  override def compare(x: (Expr, String), y: (Expr, String)): Int = 0
}

object ParameterNameGenerator {
  def generate(n: String): Param = Param(n)
}
