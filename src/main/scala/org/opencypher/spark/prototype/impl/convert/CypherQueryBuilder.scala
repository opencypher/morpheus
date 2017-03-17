package org.opencypher.spark.prototype.impl.convert

import java.util.concurrent.atomic.AtomicLong

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.ir._
import org.opencypher.spark.prototype.api.ir.block._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.{AllGiven, Pattern}

object CypherQueryBuilder {
  def from(s: Statement, q: String, tokenDefs: GlobalsRegistry): CypherQuery[Expr] = {
    val builder = new CypherQueryBuilder(q, tokenDefs)
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

class CypherQueryBuilder(query: String, tokenDefs: GlobalsRegistry) {
  val exprConverter = new ExpressionConverter(tokenDefs)
  val patternConverter = new PatternConverter(tokenDefs)

  var firstBlock: Option[BlockRef] = None

  def add(c: Clause, blockRegistry: BlockRegistry[Expr]): BlockRegistry[Expr] = {
    c match {
      case Match(_, pattern, _, astWhere) =>
        // TODO: labels are not inside the pattern anymore here -- need to consider that
        val given = convertPattern(pattern)
        val where = convertWhere(astWhere)

        val after = blockRegistry.reg.headOption.map(_._1).toSet
        val block = MatchBlock[Expr](after, given, where)
        val (ref, reg) = blockRegistry.register(block)
        reg

      case With(_, _, _, _, _, _) =>
        throw new IllegalArgumentException("With")

      case Return(_, ReturnItems(_, items), _, _, _, _) =>
        val yields = ProjectedFields[Expr](items.map {
          case AliasedReturnItem(e, v) => {
            val expr = convertExpr(e)
            Field(v.name) -> expr
          }
          case UnaliasedReturnItem(e, t) => {
            val expr = convertExpr(e)
            Field(expr.toString) -> expr
          }
        }.toMap)

      val after = blockRegistry.reg.headOption.map(_._1).toSet
        val projs = ProjectBlock[Expr](after = after, where = AllGiven[Expr](), binds = yields)

        val (ref, reg) = blockRegistry.register(projs)
        // TODO: Add rewriter and put the above in case With(...)

        // TODO: Figure out nodes and relationships
        val rItems: Seq[Field] = items.map(extract)
        val returns = ResultBlock[Expr](Set(ref), OrderedFields(rItems), Set.empty, Set.empty)

        val (_, reg2) = reg.register(returns)
        reg2
    }
  }

  private def extract(r: ReturnItem): Field = {
    r match {
      case AliasedReturnItem(expr, variable) => Field(variable.name)
      case UnaliasedReturnItem(expr, text) => Field(convertExpr(expr).toString)
    }
  }

  private def convertPattern(p: ast.Pattern): Pattern[Expr] = patternConverter.convert(p)
  private def convertExpr(e: ast.Expression): Expr = exprConverter.convert(e)

  private def convertWhere(where: Option[ast.Where]): AllGiven[Expr] = where match {
    case Some(ast.Where(expr)) => convertExpr(expr) match {
      case org.opencypher.spark.prototype.api.expr.Ands(exprs) => AllGiven(exprs)
      case e => AllGiven(Set(e))
    }
    case None => AllGiven[Expr]()
  }

  def build(blocks: BlockRegistry[Expr]): CypherQuery[Expr] = {

    val (ref, r) = blocks.reg.collectFirst {
      case (_ref, r: ResultBlock[Expr]) => _ref -> r
    }.get

    val model = QueryModel(r, tokenDefs, blocks.reg.toMap - ref)

    val info = QueryInfo(query)

    CypherQuery(info, model)
  }
}

object fieldOrdering extends Ordering[(Field, String)] {
  override def compare(x: (Field, String), y: (Field, String)): Int = 0
}

object exprOrdering extends Ordering[(Expr, String)] {
  override def compare(x: (Expr, String), y: (Expr, String)): Int = 0
}
