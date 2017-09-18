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
package org.opencypher.caps.ir.impl

import cats.implicits._
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, ast}
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.util.parsePathOrURI
import org.opencypher.caps.impl.CompilationStage
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.block.{SortItem, _}
import org.opencypher.caps.ir.api.global.GlobalsRegistry
import org.opencypher.caps.ir.api.pattern.{AllGiven, Pattern}
import org.opencypher.caps.ir.impl.instances._

object IRBuilder extends CompilationStage[ast.Statement, CypherQuery[Expr], IRBuilderContext] {

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
            val uri = getNamedGraph(c, context)
            val blockRegistry = context.blocks
            val after = blockRegistry.lastAdded.toSet
            val block = MatchBlock[Expr](after, pattern, given, optional, uri)

            implicit val globals: GlobalsRegistry = context.globals
            val typedOutputs = typedMatchBlock.outputs(block)

            val (ref, reg) = blockRegistry.register(block)
            val updatedContext = context.withBlocks(reg).withFields(typedOutputs)
            put[R, IRBuilderContext](updatedContext) >> pure[R, Vector[BlockRef]](Vector(ref))
          }
        } yield refs

      case ast.With(_, ast.ReturnItems(_, items), GraphReturnItems(_, gItems), orderBy, skip, limit, where) if !items.exists(_.expression.containsAggregate) =>
        for {
          fieldExprs <- EffMonad[R].sequence(items.map(convertReturnItem[R]).toVector)
          graphs <- EffMonad[R].sequence(gItems.map(convertGraphReturnItem[R]).toVector)
          given <- convertWhere(where)
          context <- get[R, IRBuilderContext]
          refs <- {
            val namedGraph = getNamedGraph(c, context)
            val (projectRef, projectReg) = registerProjectBlock(context, fieldExprs, graphs, given, namedGraph)
            val appendList = (list: Vector[BlockRef]) => pure[R, Vector[BlockRef]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R,IRBuilderContext](context.copy(blocks = projectReg)) >> orderAndSliceBlock >>= appendList
          }
        } yield refs

      case ast.With(_, ast.ReturnItems(_, items), GraphReturnItems(_, Seq()), _, _, _, None) =>
        for {
          fieldExprs <- EffMonad[R].sequence(items.map(convertReturnItem[R]).toVector)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (agg, group) = fieldExprs.partition {
              case (_, _: Aggregator) => true
            }

            val (ref1, reg1) = registerProjectBlock(context, group, source = context.ambientGraph)
            val after = reg1.lastAdded.toSet
            val aggBlock = AggregationBlock[Expr](after, Aggregations(agg.toSet), group.map(_._1).toSet, context.ambientGraph)
            val (ref2, reg2) = reg1.register(aggBlock)

            put[R, IRBuilderContext](context.copy(blocks = reg2)) >> pure[R, Vector[BlockRef]](Vector(ref1, ref2))
          }
        } yield refs

        // TODO: Figure out how to share code with the below where GraphReturnItems are present
      case ast.Return(distinct, ast.ReturnItems(_, items), None, _, _, _, _) =>
        for {
          fieldExprs <- EffMonad[R].sequence(items.map(convertReturnItem[R]).toVector)
          context <- get[R, IRBuilderContext]
          refs <- {
            val namedGraph = getNamedGraph(c, context)
            val (ref, reg) = registerProjectBlock(context, fieldExprs, distinct = distinct, source = namedGraph)
            val rItems = fieldExprs.map(_._1)
            val orderedFields = OrderedFieldsAndGraphs[Expr](rItems)
            val returns = ResultBlock[Expr](Set(ref), orderedFields, Set.empty, Set.empty, context.ambientGraph)
            val (ref2, reg2) = reg.register(returns)
            put[R, IRBuilderContext](context.copy(blocks = reg2)) >> pure[R, Vector[BlockRef]](Vector(ref, ref2))
          }
        } yield refs

      case ast.Return(distinct, ast.ReturnItems(_, items), Some(GraphReturnItems(_, gItems)), _, _, _, _) =>
        for {
          fieldExprs <- EffMonad[R].sequence(items.map(convertReturnItem[R]).toVector)
          graphs <- EffMonad[R].sequence(gItems.map(convertGraphReturnItem[R]).toVector)
          context <- get[R, IRBuilderContext]
          refs <- {
            val namedGraph = getNamedGraph(c, context)
            val (ref, reg) = registerProjectBlock(context, fieldExprs, distinct = distinct, source = namedGraph, graphs = graphs)
            val rItems = fieldExprs.map(_._1)
            val orderedFields = OrderedFieldsAndGraphs[Expr](rItems, graphs.toSet)
            val returns = ResultBlock[Expr](Set(ref), orderedFields, Set.empty, Set.empty, context.ambientGraph)
            val (ref2, reg2) = reg.register(returns)
            put[R, IRBuilderContext](context.copy(blocks = reg2)) >> pure[R, Vector[BlockRef]](Vector(ref, ref2))
          }
        } yield refs

      case x =>
        error(IRBuilderError(s"Clause not yet supported: $x"))(Vector.empty[BlockRef])
    }
  }

  private def getNamedGraph(c: Clause, context: IRBuilderContext) = {
    context.semanticState.recordedContextGraphs.get(c).map {
      c => context.graphs(c.source)
    }.getOrElse(context.ambientGraph)
  }

  private def registerProjectBlock(context: IRBuilderContext, fieldExprs: Vector[(IRField, Expr)], graphs: Seq[NamedGraph] = Seq.empty, given: AllGiven[Expr] = AllGiven[Expr](), source: NamedGraph, distinct: Boolean = false) = {
    val blockRegistry = context.blocks
    val binds = FieldsAndGraphs(fieldExprs.toMap, graphs.toSet)

    val after = blockRegistry.lastAdded.toSet
    val projs = ProjectBlock[Expr](after, binds, given, source, distinct)

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

          val orderAndSliceBlock = OrderAndSliceBlock[Expr](after, sortItems, skipExpr, limitExpr, context.ambientGraph)
          val (ref,reg) = blockRegistry.register(orderAndSliceBlock)
          put[R, IRBuilderContext](context.copy(blocks = reg)) >> pure[R, Vector[BlockRef]](Vector(ref))
        }
      }
    } yield refs
  }

  private def convertGraphReturnItem[R: _mayFail : _hasContext](item: ast.GraphReturnItem): Eff[R, NamedGraph] = item match {
    case ast.NewContextGraphs(source: GraphAtAs, _) =>
      for {
        context <- get[R, IRBuilderContext]
        out <- {
          val graphVar = source.as.getOrElse(Raise.impossible("graph not named"))

          // TODO: Bug in frontend, fixed by PR <insert-pr-number-when-done>
          val escapedGraphname = fix(graphVar.name)

          val graphURI = source.at.url match {
            case Left(parameter) =>
              Raise.notYetImplemented("graph uris by parameter")
            case Right(literal) =>
              parsePathOrURI(literal.value)
          }

          val namedGraph = NamedGraph(escapedGraphname, graphURI)

          val newContext = context.withGraphAt(namedGraph)

          put[R, IRBuilderContext](newContext) >> pure[R, NamedGraph](namedGraph)
        }
      } yield out


    case ast.NewContextGraphs(source, _) =>
      Raise.notYetImplemented("graphs without AT")
    case ast.NewTargetGraph(target) =>
      ???
    case ast.ReturnedGraph(inner) =>
      for {
        context <- get[R, IRBuilderContext]
        out <- {
          val g = context.graphs(fix(inner.as.getOrElse(Raise.impossible("graph didn't have a name!")).name))
          pure[R, NamedGraph](g)
        }
      } yield out
  }

  // The namespacer in Neo4j frontend incorrectly renames graph variables to avoid shadowing -- this is not fine for returned graphs
  private def fix(graphName: String) = {
    if (graphName.startsWith(" "))
      graphName.substring(2, graphName.indexOf("@"))
    else graphName
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
