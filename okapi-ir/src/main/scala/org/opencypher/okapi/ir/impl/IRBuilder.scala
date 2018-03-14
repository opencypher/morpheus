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
package org.opencypher.okapi.ir.impl

import cats.implicits._
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.v3_4.{expressions => exp}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block.{SortItem, _}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern.Pattern
import org.opencypher.okapi.ir.api.set.{SetItem, SetLabelItem, SetPropertyItem}
import org.opencypher.okapi.ir.api.util.CompilationStage
import org.opencypher.okapi.ir.impl.refactor.instances._

object IRBuilder extends CompilationStage[ast.Statement, CypherQuery[Expr], IRBuilderContext] {

  override type Out = Either[IRBuilderError, (Option[CypherQuery[Expr]], IRBuilderContext)]

  override def process(input: ast.Statement)(implicit context: IRBuilderContext): Out =
    buildIR[IRBuilderStack[Option[CypherQuery[Expr]]]](input).run(context)

  override def extract(output: Out): CypherQuery[Expr] =
    output match {
      case Left(error) => throw IllegalStateException(s"Error during IR construction: $error")
      case Right((Some(q), _)) => q
      case Right((None, _)) => throw IllegalStateException(s"Failed to construct IR")
    }

  private def buildIR[R: _mayFail : _hasContext](s: ast.Statement): Eff[R, Option[CypherQuery[Expr]]] =
    s match {
      case ast.Query(_, part) =>
        for {
          query <- {
            part match {
              case ast.SingleQuery(clauses) =>
                val blocks = clauses.toVector.traverse(convertClause[R])
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

      case ast.UseGraph(qgn: ast.QualifiedGraphName) =>
        for {
          context <- get[R, IRBuilderContext]
          refs <- {
            val irQgn = QualifiedGraphName(qgn.parts)
            val ds = context.resolver(irQgn.namespace)
            val schema = ds.schema(irQgn.graphName) match {
              case Some(s) => s
              case None => ds.graph(irQgn.graphName).schema
            }
            val irGraph = IRCatalogGraph(irQgn, schema)
            val updatedContext = context.withWorkingGraph(irGraph)
            put[R, IRBuilderContext](updatedContext) >> pure[R, Vector[BlockRef]](Vector.empty)
          }
        } yield refs

      case ast.Match(optional, pattern, _, astWhere) =>
        for {
          pattern <- convertPattern(pattern)
          given <- convertWhere(astWhere)
          context <- get[R, IRBuilderContext]
          refs <- {
            val blockRegistry = context.blocks
            val after = blockRegistry.lastAdded.toSet
            val block = MatchBlock[Expr](after, pattern, given, optional, context.workingGraph)

            val typedOutputs = typedMatchBlock.outputs(block)
            val (ref, reg) = blockRegistry.register(block)
            val updatedContext = context.withBlocks(reg).withFields(typedOutputs)
            put[R, IRBuilderContext](updatedContext) >> pure[R, Vector[BlockRef]](Vector(ref))
          }
        } yield refs

      case ast.With(distinct, ast.ReturnItems(_, items), orderBy, skip, limit, where)
        if !items.exists(_.expression.containsAggregate) =>
        for {
          fieldExprs <- items.toVector.traverse(convertReturnItem[R])
          given <- convertWhere(where)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (projectRef, projectReg) =
              registerProjectBlock(context, fieldExprs, given, context.workingGraph, distinct = distinct)
            val appendList = (list: Vector[BlockRef]) => pure[R, Vector[BlockRef]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R, IRBuilderContext](context.copy(blocks = projectReg)) >> orderAndSliceBlock flatMap appendList
          }
        } yield refs

      case ast.With(distinct, ast.ReturnItems(_, items), _, _, _, None) =>
        for {
          fieldExprs <- items.toVector.traverse(convertReturnItem[R])
          context <- get[R, IRBuilderContext]
          refs <- {
            val (agg, group) = fieldExprs.partition {
              case (_, _: Aggregator) => true
              case _ => false
            }

            val (ref1, reg1) = registerProjectBlock(context, group, source = context.workingGraph, distinct = distinct)
            val after = reg1.lastAdded.toSet
            val aggBlock =
              AggregationBlock[Expr](after, Aggregations(agg.toSet), group.map(_._1).toSet, context.workingGraph)
            val (ref2, reg2) = reg1.register(aggBlock)

            put[R, IRBuilderContext](context.copy(blocks = reg2)) >> pure[R, Vector[BlockRef]](Vector(ref1, ref2))
          }
        } yield refs

      case ast.Unwind(listExpression, variable) =>
        for {
          tuple <- convertUnwindItem(listExpression, variable)
          context <- get[R, IRBuilderContext]
          block <- {
            val (list, item) = tuple
            val binds: UnwoundList[Expr] = UnwoundList(list, item)
            val block = UnwindBlock(context.blocks.lastAdded.toSet, binds, context.workingGraph)
            val (ref, reg) = context.blocks.register(block)

            put[R, IRBuilderContext](context.copy(blocks = reg)) >> pure[R, Vector[BlockRef]](Vector(ref))
          }
        } yield block

      // TODO: Support merges, deletes
      case ast.ConstructGraph(Nil, creates, Nil, sets) =>
        for {
          patterns <- creates.map { case ast.Create(p: exp.Pattern) => p }.traverse(convertPattern[R])
          setItems <- sets.flatMap { case ast.SetClause(s: Seq[ast.SetItem]) => s }.traverse(convertSetItem[R])
          context <- get[R, IRBuilderContext]
          refs <- {
            val pattern = patterns.foldLeft(Pattern.empty[Expr])(_ ++ _)
            val patternSchema = context.workingGraph.schema.forPattern(pattern)
            val (schema, _) = setItems.foldLeft(patternSchema -> Map.empty[Var, CypherType]) { case ((currentSchema, rewrittenVarTypes), setItem: SetItem[Expr]) =>
              setItem match {
                case SetLabelItem(variable, labels) =>
                  val existingLabels = rewrittenVarTypes.getOrElse(variable, variable.cypherType) match {
                    case CTNode(existing) => existing
                    case other => throw UnsupportedOperationException(s"Setting a labels on something that is not a node: $other")
                  }
                  val labelsAfterSet = existingLabels ++ labels
                   val updatedSchema = currentSchema
                      .dropPropertiesFor(existingLabels)
                      .withNodePropertyKeys(labelsAfterSet, patternSchema.nodeKeys(existingLabels))
                  updatedSchema -> rewrittenVarTypes.updated(variable, CTNode(labelsAfterSet))
                case SetPropertyItem(propertyKey, variable, setValue) =>
                  val propertyType = setValue.cypherType
                  val updatedSchema = variable.cypherType match {
                    case CTNode(labels) =>
                      val allRelevantLabelCombinations = currentSchema.combinationsFor(labels)
                      val property = if (allRelevantLabelCombinations.size == 1) propertyType else propertyType.nullable
                      allRelevantLabelCombinations.foldLeft(currentSchema) { case (innerCurrentSchema, combo) =>
                        val updatedPropertyKeys = innerCurrentSchema.keysFor(Set(combo)).updated(propertyKey, property)
                        innerCurrentSchema.withOverwrittenNodePropertyKeys(combo, updatedPropertyKeys)
                      }
                    case CTRelationship(types) =>
                      val typesToUpdate = if (types.isEmpty) currentSchema.relationshipTypes else types
                      typesToUpdate.foldLeft(currentSchema) { case (innerCurrentSchema, relType) =>
                        val updatedPropertyKeys = innerCurrentSchema.relationshipKeys(relType).updated(propertyKey, propertyType)
                        innerCurrentSchema.withOverwrittenRelationshipPropertyKeys(relType, updatedPropertyKeys)
                      }
                    case other => throw IllegalArgumentException("node or relationship to set a property on", other)
                  }
                  updatedSchema -> rewrittenVarTypes
              }
            }
            val patternGraph = IRPatternGraph[Expr](schema, pattern, setItems.collect { case p: SetPropertyItem[Expr] => p })
            val updatedContext = context.withWorkingGraph(patternGraph)
            put[R, IRBuilderContext](updatedContext) >> pure[R, Vector[BlockRef]](Vector.empty)
          }
        } yield refs

      case ast.ReturnGraph(qgnOpt) =>
        for {
          context <- get[R, IRBuilderContext]
          refs <- {
            val after = context.blocks.lastAdded.toSet
            val irGraph = qgnOpt match {
              case None => context.workingGraph
              case Some(astQgn) =>
                val irQgn = QualifiedGraphName(astQgn.parts)
                val pgds = context.resolver(irQgn.namespace)
                IRCatalogGraph(irQgn, pgds.schema(irQgn.graphName).getOrElse(pgds.graph(irQgn.graphName).schema))
            }
            val returns = GraphResultBlock[Expr](after, irGraph)
            val (ref, updatedReg) = context.blocks.register(returns)
            put[R, IRBuilderContext](context.copy(blocks = updatedReg)) >> pure[R, Vector[BlockRef]](Vector(ref))
          }
        } yield refs

      case ast.Return(distinct, ast.ReturnItems(_, items), orderBy, skip, limit, _) =>
        for {
          fieldExprs <- items.toVector.traverse(convertReturnItem[R])
          context <- get[R, IRBuilderContext]
          refs <- {
            val (projectRef, projectReg) =
              registerProjectBlock(context, fieldExprs, distinct = distinct, source = context.workingGraph)
            val appendList = (list: Vector[BlockRef]) => pure[R, Vector[BlockRef]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R, IRBuilderContext](context.copy(blocks = projectReg)) >> orderAndSliceBlock flatMap appendList
          }
          context2 <- get[R, IRBuilderContext]
          refs2 <- {
            val rItems = fieldExprs.map(_._1)
            val orderedFields = OrderedFields[Expr](rItems)
            val result = TableResultBlock[Expr](Set(refs.last), orderedFields, Set.empty, Set.empty, context.workingGraph)
            val (resultRef, resultReg) = context2.blocks.register(result)
            put[R, IRBuilderContext](context.copy(blocks = resultReg)) >> pure[R, Vector[BlockRef]](refs :+ resultRef)
          }
        } yield refs2

      case x =>
        error(IRBuilderError(s"Clause not yet supported: $x"))(Vector.empty[BlockRef])
    }
  }

  private def registerProjectBlock(
    context: IRBuilderContext,
    fieldExprs: Vector[(IRField, Expr)],
    given: Set[Expr] = Set.empty[Expr],
    source: IRGraph,
    distinct: Boolean): (BlockRef, BlockRegistry[Expr]) = {
    val blockRegistry = context.blocks
    val binds = Fields(fieldExprs.toMap)

    val after = blockRegistry.lastAdded.toSet
    val projs = ProjectBlock[Expr](after, binds, given, source, distinct)

    blockRegistry.register(projs)
  }

  private def registerOrderAndSliceBlock[R: _mayFail : _hasContext](
    orderBy: Option[ast.OrderBy],
    skip: Option[ast.Skip],
    limit: Option[ast.Limit]) = {
    for {
      context <- get[R, IRBuilderContext]
      sortItems <- orderBy match {
        case Some(ast.OrderBy(sortItems)) =>
          sortItems.toVector.traverse(convertSortItem[R])
        case None => Vector[ast.SortItem]().traverse(convertSortItem[R])
      }
      skipExpr <- convertExpr(skip.map(_.expression))
      limitExpr <- convertExpr(limit.map(_.expression))

      refs <- {
        if (sortItems.isEmpty && skipExpr.isEmpty && limitExpr.isEmpty) pure[R, Vector[BlockRef]](Vector())
        else {
          val blockRegistry = context.blocks
          val after = blockRegistry.lastAdded.toSet

          val orderAndSliceBlock = OrderAndSliceBlock[Expr](after, sortItems, skipExpr, limitExpr, context.workingGraph)
          val (ref, reg) = blockRegistry.register(orderAndSliceBlock)
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

    case ast.UnaliasedReturnItem(e, name) =>
      for {
        expr <- convertExpr(e)
        context <- get[R, IRBuilderContext]
        field <- {
          val field = IRField(name)(expr.cypherType)
          put[R, IRBuilderContext](context.withFields(Set(field))) >> pure[R, IRField](field)
        }
      } yield field -> expr

    case _ =>
      throw IllegalArgumentException(s"${ast.AliasedReturnItem.getClass} or ${ast.UnaliasedReturnItem.getClass}", item.getClass)
  }

  private def convertUnwindItem[R: _mayFail : _hasContext](
    list: exp.Expression,
    variable: exp.Variable): Eff[R, (Expr, IRField)] = {
    for {
      expr <- convertExpr(list)
      context <- get[R, IRBuilderContext]
      typ <- expr.cypherType.material match {
        case CTList(inner) =>
          pure[R, CypherType](inner)
        case CTAny =>
          pure[R, CypherType](CTAny)
        case x =>
          error(IRBuilderError(s"unwind expression was not a list: $x"))(CTWildcard: CypherType)
      }
      field <- {
        val field = IRField(variable.name)(typ)
        put[R, IRBuilderContext](context.withFields(Set(field))) >> pure[R, (Expr, IRField)](expr -> field)
      }
    } yield field
  }

  private def convertPattern[R: _hasContext](p: exp.Pattern): Eff[R, Pattern[Expr]] = {
    for {
      context <- get[R, IRBuilderContext]
      result <- {
        val pattern = context.convertPattern(p)
        val patternTypes = pattern.fields.foldLeft(context.knownTypes) {
          case (acc, f) => acc.updated(exp.Variable(f.name)(InputPosition.NONE), f.cypherType)
        }
        put[R, IRBuilderContext](context.copy(knownTypes = patternTypes)) >> pure[R, Pattern[Expr]](pattern)
      }
    } yield result
  }

  private def convertSetItem[R: _mayFail : _hasContext](p: ast.SetItem): Eff[R, SetItem[Expr]] = {
    p match {
      case ast.SetPropertyItem(exp.LogicalProperty(map: exp.Variable, exp.PropertyKeyName(propertyName)), setValue: exp.Expression) =>
        for {
          variable <- convertExpr[R](map)
          convertedSetExpr <- convertExpr[R](setValue)
          result <- {
            val setItem = SetPropertyItem(propertyName, variable.asInstanceOf[Var], convertedSetExpr)
            pure[R, SetItem[Expr]](setItem)
          }
        } yield result
      case ast.SetLabelItem(expr, labels) =>
        for {
          variable <- convertExpr[R](expr)
          result <- {
            val setLabel: SetItem[Expr] = SetLabelItem(variable.asInstanceOf[Var], labels.map(_.name).toSet)
            pure[R, SetItem[Expr]](setLabel)
          }
        } yield result
      case unsupported => throw UnsupportedOperationException(s"conversion of $unsupported")
    }
  }

  private def convertExpr[R: _mayFail : _hasContext](e: Option[exp.Expression]): Eff[R, Option[Expr]] =
    for {
      context <- get[R, IRBuilderContext]
    } yield
      e match {
        case Some(expr) => Some(context.convertExpression(expr))
        case None => None
      }

  private def convertExpr[R: _mayFail : _hasContext](e: exp.Expression): Eff[R, Expr] =
    for {
      context <- get[R, IRBuilderContext]
    } yield context.convertExpression(e)

  private def convertWhere[R: _mayFail : _hasContext](where: Option[ast.Where]): Eff[R, Set[Expr]] = where match {
    case Some(ast.Where(expr)) =>
      for {
        predicate <- convertExpr(expr)
      } yield {
        predicate match {
          case org.opencypher.okapi.ir.api.expr.Ands(exprs) => exprs
          case e => Set(e)
        }
      }

    case None =>
      pure[R, Set[Expr]](Set.empty[Expr])
  }

  private def convertRegistry[R: _mayFail : _hasContext]: Eff[R, Option[CypherQuery[Expr]]] =
    for {
      context <- get[R, IRBuilderContext]
    } yield {
      val blocks = context.blocks
      val (ref, r) = blocks.reg.collectFirst {
        case (_ref, r: ResultBlock[Expr]) => _ref -> r
      }.get

      val model = QueryModel(r, context.parameters, blocks.reg.toMap - ref)
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
