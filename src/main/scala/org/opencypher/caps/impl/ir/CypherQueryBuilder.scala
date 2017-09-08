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
package org.opencypher.caps.impl.ir

import cats.implicits._
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Limit, OrderBy, Skip, Statement}
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, ast}
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.ir._
import org.opencypher.caps.api.ir.block.{SortItem, _}
import org.opencypher.caps.api.ir.pattern.{AllGiven, Pattern}
import org.opencypher.caps.impl.CompilationStage
import org.opencypher.caps.impl.instances.ir.block.expr._
import org.opencypher.caps.impl.syntax.block._

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
      case ast.Match(optional, pattern, _, astWhere) =>
        for {
          pattern <- convertPattern(pattern)
          given <- convertWhere(astWhere)
          context <- get[R, IRBuilderContext]
          refs <- {
            val blockRegistry = context.blocks
            val after = blockRegistry.lastAdded.toSet
            val block = MatchBlock[Expr](after, pattern, given, optional, context.graphBlock)

            implicit val globals = context.globals
            val typedOutputs = block.outputs

            val (ref, reg) = blockRegistry.register(block)
            val updatedContext = context.withBlocks(reg).withFields(typedOutputs)
            put[R, IRBuilderContext](updatedContext) >> pure[R, Vector[BlockRef]](Vector(ref))
          }
        } yield refs

      case ast.With(_, ast.ReturnItems(_, items), _, orderBy, skip, limit, where) if !items.exists(_.expression.containsAggregate) =>
        for {
          fieldExprs <- EffMonad[R].sequence(items.map(convertReturnItem[R]).toVector)
          given <- convertWhere(where)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (projectRef, projectReg) = registerProjectBlock(context, fieldExprs, given)
            val appendList = (list: Vector[BlockRef]) => pure[R, Vector[BlockRef]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R,IRBuilderContext](context.copy(blocks = projectReg)) >> orderAndSliceBlock >>= appendList
          }
        } yield refs

      case ast.With(_, ast.ReturnItems(_, items), _, _, _, _, None) =>
        for {
          fieldExprs <- EffMonad[R].sequence(items.map(convertReturnItem[R]).toVector)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (agg, group) = fieldExprs.partition {
              case (_, _: Aggregator) => true
            }

            val (ref1, reg1) = registerProjectBlock(context, group)
            val after = reg1.lastAdded.toSet
            val aggBlock = AggregationBlock[Expr](after, Aggregations(agg.toSet), group.map(_._1).toSet)
            val (ref2, reg2) = reg1.register(aggBlock)

            put[R, IRBuilderContext](context.copy(blocks = reg2)) >> pure[R, Vector[BlockRef]](Vector(ref1, ref2))
          }
        } yield refs

      case ast.Return(distinct, ast.ReturnItems(_, items), _, _, _, _) =>
        for {
          fieldExprs <- EffMonad[R].sequence(items.map(convertReturnItem[R]).toVector)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (ref, reg) = registerProjectBlock(context, fieldExprs, distinct = distinct)
            val rItems = fieldExprs.map(_._1)
            val orderedFields = OrderedFields[Expr](rItems)
            val returns = ResultBlock[Expr](Set(ref), orderedFields, Set.empty, Set.empty, context.graphBlock)
            val (ref2, reg2) = reg.register(returns)
            put[R, IRBuilderContext](context.copy(blocks = reg2)) >> pure[R, Vector[BlockRef]](Vector(ref, ref2))
          }
        } yield refs

      case x =>
        error(IRBuilderError(s"Clause not yet supported: $x"))(Vector.empty[BlockRef])
    }
  }

  private def registerProjectBlock(context: IRBuilderContext, fieldExprs: Vector[(Field, Expr)], given: AllGiven[Expr] = AllGiven[Expr](), distinct: Boolean = false) = {
    val blockRegistry = context.blocks
    val binds = ProjectedFields(fieldExprs.toMap)

    val after = blockRegistry.lastAdded.toSet
    val projs = ProjectBlock[Expr](after, binds, given, graph = context.graphBlock, distinct)

    blockRegistry.register(projs)
  }

  private def registerOrderAndSliceBlock[R: _mayFail : _hasContext](orderBy: Option[OrderBy],
                                                                    skip: Option[Skip],
                                                                    limit: Option[Limit]) = {
    for {
      context <- get[R, IRBuilderContext]
      sortItems <- orderBy match {
        case Some(ast.OrderBy(sortItems)) =>
          EffMonad[R].sequence(sortItems.map(convertSortItem[R]).toVector)
        case None => EffMonad[R].sequence(Vector[Eff[R,SortItem[Expr]]]())
      }
      skipExpr <- convertExpr(skip.map(_.expression))
      limitExpr <- convertExpr(limit.map(_.expression))

      refs <- {
        if (sortItems.isEmpty && skipExpr.isEmpty && limitExpr.isEmpty) pure[R, Vector[BlockRef]](Vector())
        else {
          val blockRegistry = context.blocks
          val after = blockRegistry.lastAdded.toSet

          val orderAndSliceBlock = OrderAndSliceBlock[Expr](after, sortItems, skipExpr, limitExpr, context.graphBlock)
          val (ref,reg) = blockRegistry.register(orderAndSliceBlock)
          put[R, IRBuilderContext](context.copy(blocks = reg)) >> pure[R, Vector[BlockRef]](Vector(ref))
        }
      }
    } yield refs
  }

  private def convertReturnItem[R: _mayFail : _hasContext](item: ast.ReturnItem): Eff[R, (IRField, Expr)] = item match {

    case ast.AliasedReturnItem(e, v) =>
      for {
        expr <- convertExpr(e)
        context <- get[R, IRBuilderContext]
        field <- {
          val field = IRField(v.name)(expr.cypherType)
          put[R, IRBuilderContext](context.withFields(Set(field))) >> pure[R, IRField](field)
        }
      } yield field -> expr

    case ast.UnaliasedReturnItem(e, t) =>
      error(IRBuilderError(s"Did not expect unnamed return item"))(IRField(t)() -> Var(t)())

  }

  private def convertPattern[R: _mayFail : _hasContext](p: ast.Pattern): Eff[R, Pattern[Expr]] = {
    for {
      context <- get[R, IRBuilderContext]
      result <- {
        val pattern = context.convertPattern(p)
        val patternTypes = pattern.fields.foldLeft(context.knownTypes) {
          case (acc, f) => {
            acc.updated(ast.Variable(f.name)(InputPosition.NONE), f.cypherType)
          }
        }
        put[R, IRBuilderContext](context.copy(knownTypes = patternTypes)) >> pure[R, Pattern[Expr]](pattern)
      }
    } yield result
  }

  private def convertExpr[R: _mayFail : _hasContext](e: Option[ast.Expression]): Eff[R, Option[Expr]] =
    for {
      context <- get[R, IRBuilderContext]
    } yield e match {
      case Some(expr) => Some(context.convertExpression(expr))
      case None => None
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
          case org.opencypher.caps.api.expr.Ands(exprs) => AllGiven(exprs)
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

  private def getKnownFields(blockRefs: Set[BlockRef], registry: BlockRegistry[Expr]): Set[IRField] = {
    blockRefs.map(registry.apply).map { block =>
      block.binds.fields ++ getKnownFields(block.after, registry)
    }.reduceOption(_ ++ _).getOrElse(Set.empty)
  }

  private def convertSortItem[R: _mayFail : _hasContext](item: ast.SortItem): Eff[R, SortItem[Expr]] = {
    item match {
      case ast.AscSortItem(astExpr) =>
        for {
          expr <- convertExpr(astExpr)
        } yield Asc(expr)
      case ast.DescSortItem(astExpr) =>
        for {
          expr <- convertExpr(astExpr)
        } yield Desc(expr)
    }
  }
}
