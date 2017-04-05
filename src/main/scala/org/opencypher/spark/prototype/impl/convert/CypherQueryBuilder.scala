package org.opencypher.spark.prototype.impl.convert

import java.util.concurrent.atomic.AtomicLong

import cats.data._
import cats.implicits._
import cats.{Foldable, Monad, Monoid}
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, ast}
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.types.CTWildcard
import org.opencypher.spark.impl.types.{SchemaTyper, TyperContext, TyperError, TyperResult}
import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.ir._
import org.opencypher.spark.prototype.api.ir.block._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.{AllGiven, Pattern}
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.prototype.impl.convert.types.{_fails, _hasContext}
import org.opencypher.spark.prototype.impl.convert.types._

object CypherQueryBuilder {
  def buildIROrThrow(s: ast.Statement, context: IRBuilderContext): CypherQuery[Expr] = {
    constructIR(s, context) match {
      case Left(error) => throw new IllegalStateException(s"Error during IR construction: $error")
      case Right(q) => q
    }
  }

  def constructIR(s: ast.Statement, context: IRBuilderContext): Either[IRBuilderError, CypherQuery[Expr]] = {
    val builder = new CypherQueryBuilder(context.queryString, context.globals)

    val irFromClauses = s match {
      case ast.Query(_, part) => part match {
        case ast.SingleQuery(clauses) =>
          val clause1 = clauses.head
          val clause2 = clauses(1)
          val step1: Eff[IRBuilderStack[BlockRegistry[Expr]], BlockRegistry[Expr]] = builder.add(clause1, BlockRegistry.empty[Expr])

          step1.run(context) match {
            case l@Left(_) => l
            case Right((reg, nextContext)) =>
              val next: Eff[IRBuilderStack[BlockRegistry[Expr]], BlockRegistry[Expr]] = builder.add(clause2, reg)
              next.run(nextContext)
          }
      }
      case x => throw new NotImplementedError(s"Statement not yet supported: $x")
    }

    irFromClauses match {
      case Left(error) => Left(error)
      case Right((blockReg, _)) => Right(builder.build(blockReg))
    }
  }
}

class CypherQueryBuilder(query: String, globals: GlobalsRegistry) {
  val exprConverter = new ExpressionConverter(globals)
  val patternConverter = new PatternConverter(globals)

  var firstBlock: Option[BlockRef] = None

  def add[R: _fails : _hasContext](c: ast.Clause, blockRegistry: BlockRegistry[Expr]): Eff[R, BlockRegistry[Expr]] = {

    c match {
      case ast.Match(_, pattern, _, astWhere) =>
        for {
          given <- convertPattern(pattern)
          where <- convertWhere(astWhere)
        } yield {

          val after = blockRegistry.reg.headOption.map(_._1).toSet
          val block = MatchBlock[Expr](after, given, where)
          val (ref, reg) = blockRegistry.register(block)
          reg
        }

      case ast.Return(_, ast.ReturnItems(_, items), _, _, _, _) =>
        for {
          fieldExprs <- {
            val elts: Vector[Eff[R, (Field, Expr)]] = items.map(r => convertReturnItem[R](r)).toVector
            val result = EffMonad[R].sequence(elts)
            result
          }
        } yield {

          val yields = ProjectedFields(fieldExprs.toMap)

          val after = blockRegistry.reg.headOption.map(_._1).toSet
          val projs = ProjectBlock[Expr](after = after, where = AllGiven[Expr](), binds = yields)

          val (ref, reg) = blockRegistry.register(projs)
          //         TODO: Add rewriter and put the above case in With(...)

          //         TODO: Figure out nodes and relationships
          val rItems: Seq[Field] = fieldExprs.map(_._1)
          val returns = ResultBlock[Expr](Set(ref), OrderedFields(rItems), Set.empty, Set.empty)

          val (_, reg2) = reg.register(returns)
          reg2
        }


      case x =>
        left[R, IRBuilderError, BlockRegistry[Expr]](IRBuilderError(s"Clause not yet supported: $x")) >> pure(BlockRegistry.empty[Expr])

    }
  }

  private def convertReturnItem[R: _fails : _hasContext](item: ast.ReturnItem): Eff[R, (Field, Expr)] = item match {

    case ast.AliasedReturnItem(e, v) =>
      for {
        expr <- convertExpr(e)
      } yield {
        Field(v.name)(expr.cypherType) -> expr
      }

    case ast.UnaliasedReturnItem(e, t) =>
      for {
        expr <- convertExpr(e)
      } yield {
        // TODO: should this field be named t?
        Field(expr.toString)(expr.cypherType) -> expr
      }
  }

  private def convertPattern[R: _fails : _hasContext](p: ast.Pattern): Eff[R, Pattern[Expr]] = {
    for {
      context <- get[R, IRBuilderContext]
      _ <- put[R, IRBuilderContext] {
        val pattern = patternConverter.convert(p)
        val patternTypes = pattern.fields.foldLeft(context.knownTypes) {
          case (acc, f) => acc.updated(ast.Variable(f.name)(InputPosition.NONE), f.cypherType)
        }
        context.copy(knownTypes = patternTypes)
      }
    } yield patternConverter.convert(p)
  }

  private def convertExpr[R: _fails : _hasContext](e: ast.Expression): Eff[R, Expr] = {
    for {
      context <- get[R, IRBuilderContext]
    } yield {
      val typings = context.infer(e)
      exprConverter.convert(e)(typings)
    }
  }

  private def convertWhere[R: _fails : _hasContext](where: Option[ast.Where]): Eff[R, AllGiven[Expr]] = where match {
    case Some(ast.Where(expr)) =>
      for {
        predicate <- convertExpr(expr)
      } yield {
        predicate match {
          case org.opencypher.spark.prototype.api.expr.Ands(exprs) => AllGiven(exprs)
          case e => AllGiven(Set(e))
        }
      }

    case None => pure[R, AllGiven[Expr]](AllGiven[Expr]())
  }

  def build(blocks: BlockRegistry[Expr]): CypherQuery[Expr] = {
    val (ref, r) = blocks.reg.collectFirst {
      case (_ref, r: ResultBlock[Expr]) => _ref -> r
    }.get

    val model = QueryModel(r, globals, blocks.reg.toMap - ref)

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

object BlockRegistry {
  def empty[E] = BlockRegistry[E](Seq.empty)
}

// TODO: Make this inherit from Register
case class BlockRegistry[E](reg: Seq[(BlockRef, Block[E])]) {

  def register(blockDef: Block[E]): (BlockRef, BlockRegistry[E]) = {
    val ref = BlockRef(generateName(blockDef.blockType))
    ref -> copy(reg = reg :+ ref -> blockDef)
  }

  val c = new AtomicLong()

  private def generateName(t: BlockType) = s"${t.name}_${c.incrementAndGet()}"
}

case class IRBuilderContext(queryString: String, globals: GlobalsRegistry, schema: Schema, knownTypes: Map[ast.Expression, CypherType] = Map.empty) {
  private lazy val typer = SchemaTyper(schema)

  def infer(expr: ast.Expression): Map[ast.Expression, CypherType] = {
    typer.infer(expr, TyperContext(knownTypes)) match {
      case Right(result) => result.context.typings
      case Left(errors) => throw new IllegalArgumentException(s"Some error in type inference: ${errors.toList.mkString(", ")}")
    }
  }
}

case class IRBuilderError(msg: String)
