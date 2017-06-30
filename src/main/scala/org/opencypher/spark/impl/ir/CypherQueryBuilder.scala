package org.opencypher.spark.impl.ir

import cats.implicits._
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, ast}
import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.ir._
import org.opencypher.spark.api.ir.block._
import org.opencypher.spark.api.ir.pattern.{AllGiven, Pattern}
import org.opencypher.spark.impl.CompilationStage
import org.opencypher.spark.impl.instances.ir.block.expr._
import org.opencypher.spark.impl.syntax.block._

object CypherQueryBuilder extends CompilationStage[ast.Statement, CypherQuery[Expr], IRBuilderContext] {

  override type Out = Either[IRBuilderError, (Option[CypherQuery[Expr]], IRBuilderContext)]

  override def process(input: Statement)(implicit context: IRBuilderContext): Out =
    buildIR[IRBuilderStack[Option[CypherQuery[Expr]]]](input).run(context)

  override def extract(output: Out): CypherQuery[Expr] =
    output match {
      case Left(error) => throw new IllegalStateException(s"Error during IR construction: $error")
      case Right((Some(q), _)) => q
      case Right((None, _)) => throw new IllegalStateException(s"Failed to construct IR")
    }

  private def buildIR[R: _mayFail : _hasContext](s: ast.Statement): Eff[R, Option[CypherQuery[Expr]]] =
    s match {
      case ast.Query(_, part) =>
        for {
          query <- {
            part match {
              case ast.SingleQuery(clauses) =>
                val steps = clauses.map(convertClause[R]).toVector
                val blocks = EffMonad[R].sequence(steps)
                blocks >> convertRegistry

              case x =>
                error(IRBuilderError(s"Query not supported: $x"))(None)
            }
          }
        } yield query

      case x =>
        error(IRBuilderError(s"Statement not yet supported: $x"))(None)
    }

  private def convertClause[R: _mayFail : _hasContext](c: ast.Clause): Eff[R, Vector[BlockRef]] = {

    c match {
      case ast.Match(_, pattern, _, astWhere) =>
        for {
          pattern <- convertPattern(pattern)
          given <- convertWhere(astWhere)
          context <- get[R, IRBuilderContext]
          refs <- {
            val blockRegistry = context.blocks
            val after = blockRegistry.lastAdded.toSet
            val block = MatchBlock[Expr](after, pattern, given, context.graphBlock)

            implicit val globals = context.globals
            val typedOutputs = block.outputs

            val (ref, reg) = blockRegistry.register(block)
            val updatedContext = context.withBlocks(reg).withFields(typedOutputs)
            put[R, IRBuilderContext](updatedContext) >> pure[R, Vector[BlockRef]](Vector(ref))
          }
        } yield refs

      case ast.With(_, ast.ReturnItems(_, items), _, _, _, _) =>
        for {
          fieldExprs <- EffMonad[R].sequence(items.map(convertReturnItem[R]).toVector)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (ref, reg) = registerProjectBlock(context, fieldExprs)
            put[R, IRBuilderContext](context.copy(blocks = reg)) >> pure[R, Vector[BlockRef]](Vector(ref))
          }
        } yield refs

      case ast.Return(_, ast.ReturnItems(_, items), _, _, _, _) =>
        for {
          fieldExprs <- EffMonad[R].sequence(items.map(convertReturnItem[R]).toVector)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (ref, reg) = registerProjectBlock(context, fieldExprs)

            //         TODO: Add rewriter and put the above case in With(...)
            //         TODO: Figure out nodes and relationships
            val rItems = fieldExprs.map(_._1)
            val returns = ResultBlock[Expr](Set(ref), OrderedFields(rItems), Set.empty, Set.empty, context.graphBlock)

            val (ref2, reg2) = reg.register(returns)
            put[R, IRBuilderContext](context.copy(blocks = reg2)) >> pure[R, Vector[BlockRef]](Vector(ref, ref2))
          }
        } yield refs

      case x =>
        error(IRBuilderError(s"Clause not yet supported: $x"))(Vector.empty[BlockRef])
    }
  }

  private def registerProjectBlock(context: IRBuilderContext, fieldExprs: Vector[(Field, Expr)]) = {
    val blockRegistry = context.blocks
    val binds = ProjectedFields(fieldExprs.toMap)

    val after = blockRegistry.lastAdded.toSet
    val projs = ProjectBlock[Expr](after, binds, graph = context.graphBlock)

    blockRegistry.register(projs)
  }

  private def convertReturnItem[R: _mayFail : _hasContext](item: ast.ReturnItem): Eff[R, (Field, Expr)] = item match {

    case ast.AliasedReturnItem(e, v) =>
      for {
        expr <- convertExpr(e)
        context <- get[R, IRBuilderContext]
        field <- {
          val field = Field(v.name)(expr.cypherType)
          put[R, IRBuilderContext](context.withFields(Set(field))) >> pure[R, Field](field)
        }
      } yield field -> expr

    case ast.UnaliasedReturnItem(e, t) =>
      error(IRBuilderError(s"Did not expect unnamed return item"))(Field(t)() -> Var(t)())

  }

  private def convertPattern[R: _mayFail : _hasContext](p: ast.Pattern): Eff[R, Pattern[Expr]] = {
    for {
      context <- get[R, IRBuilderContext]
      result <- {
        val pattern = context.convertPattern(p)
        val patternTypes = pattern.fields.foldLeft(context.knownTypes) {
          case (acc, f) => acc.updated(ast.Variable(f.name)(InputPosition.NONE), f.cypherType)
        }
        put[R, IRBuilderContext](context.copy(knownTypes = patternTypes)) >> pure[R, Pattern[Expr]](pattern)
      }
    } yield result
  }

  private def convertExpr[R: _mayFail : _hasContext](e: ast.Expression): Eff[R, Expr] =
    for {
      context <- get[R, IRBuilderContext]
    }
    yield context.convertExpression(e)

  private def convertWhere[R: _mayFail : _hasContext](where: Option[ast.Where]): Eff[R, AllGiven[Expr]] = where match {
    case Some(ast.Where(expr)) =>
      for {
        predicate <- convertExpr(expr)
      } yield {
        predicate match {
          case org.opencypher.spark.api.expr.Ands(exprs) => AllGiven(exprs)
          case e => AllGiven(Set(e))
        }
      }

    case None =>
      pure[R, AllGiven[Expr]](AllGiven[Expr]())
  }

  private def convertRegistry[R: _mayFail : _hasContext]: Eff[R, Option[CypherQuery[Expr]]] =
    for {
      context <- get[R, IRBuilderContext]
    } yield {
      val blocks = context.blocks
      val (ref, r) = blocks.reg.collectFirst {
        case (_ref, r: ResultBlock[Expr]) => _ref -> r
      }.get

      val model = QueryModel(r, context.globals, blocks.reg.toMap - ref, context.schemas)
      val info = QueryInfo(context.queryString)

      Some(CypherQuery(info, model))
    }
}

