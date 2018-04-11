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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.impl

import cats.implicits._
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.v3_4.{expressions => exp}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
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
import org.opencypher.okapi.ir.impl.util.VarConverters.RichIrField


object IRBuilder extends CompilationStage[ast.Statement, CypherStatement[Expr], IRBuilderContext] {

  override type Out = Either[IRBuilderError, (Option[CypherStatement[Expr]], IRBuilderContext)]

  override def process(input: ast.Statement)(implicit context: IRBuilderContext): Out =
    buildIR[IRBuilderStack[Option[CypherQuery[Expr]]]](input).run(context)

  def getContext(output: Out): IRBuilderContext = getTuple(output)._2

  private def getTuple(output: Out) =
    output match {
      case Left(error) => throw IllegalStateException(s"Error during IR construction: $error")
      case Right((None, _)) => throw IllegalStateException(s"Failed to construct IR")
      case Right(t@(Some(q), ctx)) => q -> ctx
    }

  override def extract(output: Out): CypherStatement[Expr] = getTuple(output)._1

  private def buildIR[R: _mayFail : _hasContext](s: ast.Statement): Eff[R, Option[CypherStatement[Expr]]] =
    s match {
      case ast.Query(_, part) =>
        for {
          res <- convertQueryPart(part)
        } yield res

      case ast.CreateGraph(qgn, query) =>
        for {
          innerQuery <- convertQueryPart(query)
          context <- get[R, IRBuilderContext]
          result <- {
            val schema = innerQuery.get.model.result match {
              case GraphResultBlock(_, irGraph) => context.schemaFor(irGraph.qualifiedGraphName)
              case _ => throw IllegalArgumentException("The query in CREATE GRAPH must return a graph")
            }
            val irQgn = QualifiedGraphName(qgn.parts)
            val statement = Some(CreateGraphStatement[Expr](QueryInfo(context.queryString), IRCatalogGraph(irQgn, schema), innerQuery.get))
            pure[R, Option[CypherStatement[Expr]]](statement)
          }
        } yield result

      case ast.DeleteGraph(qgn) =>
        for {
          context <- get[R, IRBuilderContext]
          result <- {
            val irQgn = QualifiedGraphName(qgn.parts)
            val schema = context.schemaFor(irQgn)
            val statement = Some(DeleteGraphStatement[Expr](QueryInfo(context.queryString), IRCatalogGraph(irQgn, schema)))
            pure[R, Option[CypherStatement[Expr]]](statement)
          }
        } yield result

      case x =>
        error(IRBuilderError(s"Statement not yet supported: $x"))(None)
    }

  private def convertQueryPart[R: _mayFail : _hasContext](part: ast.QueryPart): Eff[R, Option[CypherQuery[Expr]]] = {
    part match {
      case ast.SingleQuery(clauses) =>
        val blocks = clauses.toList.traverse(convertClause[R])
        blocks >> convertRegistry

      case x =>
        error(IRBuilderError(s"Query not supported: $x"))(None)
    }
  }

  private def convertClause[R: _mayFail : _hasContext](c: ast.Clause): Eff[R, List[Block[Expr]]] = {

    c match {

      case ast.FromGraph(qgn: ast.QualifiedGraphName) =>
        for {
          context <- get[R, IRBuilderContext]
          blocks <- {
            val irQgn = QualifiedGraphName(qgn.parts)
            val schema = context.schemaFor(irQgn)
            val irGraph = IRCatalogGraph(irQgn, schema)
            val updatedContext = context.withWorkingGraph(irGraph)
            put[R, IRBuilderContext](updatedContext) >> pure[R, List[Block[Expr]]](List.empty)
          }
        } yield blocks

      case ast.Match(optional, pattern, _, astWhere) =>
        for {
          pattern <- convertPattern(pattern)
          given <- convertWhere(astWhere)
          context <- get[R, IRBuilderContext]
          blocks <- {
            val blockRegistry = context.blockRegistry
            val after = blockRegistry.lastAdded.toList
            val block = MatchBlock[Expr](after, pattern, given, optional, context.workingGraph)

            val typedOutputs = typedMatchBlock.outputs(block)
            val updatedRegistry = blockRegistry.register(block)
            val updatedContext = context.withBlocks(updatedRegistry).withFields(typedOutputs)
            put[R, IRBuilderContext](updatedContext) >> pure[R, List[Block[Expr]]](List(block))
          }
        } yield blocks

      case ast.With(distinct, ast.ReturnItems(_, items), orderBy, skip, limit, where)
        if !items.exists(_.expression.containsAggregate) =>
        for {
          fieldExprs <- items.toList.traverse(convertReturnItem[R])
          given <- convertWhere(where)
          context <- get[R, IRBuilderContext]
          refs <- {
            val (projectRef, projectReg) =
              registerProjectBlock(context, fieldExprs, given, context.workingGraph, distinct = distinct)
            val appendList = (list: List[Block[Expr]]) => pure[R, List[Block[Expr]]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R, IRBuilderContext](context.copy(blockRegistry = projectReg)) >> orderAndSliceBlock flatMap appendList
          }
        } yield refs

      case ast.With(distinct, ast.ReturnItems(_, items), _, _, _, None) =>
        for {
          fieldExprs <- items.toList.traverse(convertReturnItem[R])
          context <- get[R, IRBuilderContext]
          blocks <- {
            val (agg, group) = fieldExprs.partition {
              case (_, _: Aggregator) => true
              case _ => false
            }

            val (projectBlock, updatedRegistry1) = registerProjectBlock(context, group, source = context.workingGraph, distinct = distinct)
            val after = updatedRegistry1.lastAdded.toList
            val aggregationBlock =
              AggregationBlock[Expr](after, Aggregations(agg.toSet), group.map(_._1).toSet, context.workingGraph)
            val updatedRegistry2 = updatedRegistry1.register(aggregationBlock)

            put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry2)) >> pure[R, List[Block[Expr]]](List(projectBlock, aggregationBlock))
          }
        } yield blocks

      case ast.Unwind(listExpression, variable) =>
        for {
          tuple <- convertUnwindItem(listExpression, variable)
          context <- get[R, IRBuilderContext]
          block <- {
            val (list, item) = tuple
            val binds: UnwoundList[Expr] = UnwoundList(list, item)
            val block = UnwindBlock(context.blockRegistry.lastAdded.toList, binds, context.workingGraph)
            val updatedRegistry = context.blockRegistry.register(block)

            put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry)) >> pure[R, List[Block[Expr]]](List(block))
          }
        } yield block

      case ast.ConstructGraph(clones, news, on) =>
        for {
          context <- get[R, IRBuilderContext]

          qgn = context.qgnGenerator.generate

          explicitCloneItems <- clones.flatMap(_.items).traverse(convertClone[R](_, qgn))

          newPatterns <- news.map {
            case ast.New(p: exp.Pattern) => p
          }.traverse(convertPattern[R](_, Some(qgn)))

          refs <- {
            val onGraphs: List[QualifiedGraphName] = on.map(graph => QualifiedGraphName(graph.parts))
            val schemaForOnGraphUnion = onGraphs.foldLeft(Schema.empty) { case (agg, next) =>
              agg ++ context.schemaFor(next)
            }

            // Computing single nodes/rels constructed by NEW (CREATE)
            // TODO: Throw exception if both clone alias and original field name are used in NEW
            val newPattern = newPatterns.foldLeft(Pattern.empty[Expr])(_ ++ _)

            // Single nodes/rels constructed by CLONE (MERGE)
            val explicitCloneItemMap = explicitCloneItems.toMap

            // Items from other graphs that are cloned by default
            val implicitCloneItems = newPattern.fields.filterNot { f =>
              f.cypherType.graph.get == qgn || explicitCloneItemMap.keys.exists(_.name == f.name)
            }
            val implicitCloneItemMap = implicitCloneItems.map { f =>
              // Convert field to clone item
              IRField(f.name)(f.cypherType.withGraph(qgn)) -> f.toVar
            }.toMap
            val cloneItemMap = implicitCloneItemMap ++ explicitCloneItemMap

            // Fields inside of CONSTRUCT could have been matched on other graphs than just the workingGraph
            val cloneSchema = schemaForEntityTypes(context, cloneItemMap.values.map(_.cypherType).toSet)

            // Make sure that there are no dangling relationships
            // we can currently only clone relationships that are also part of a new pattern
            cloneItemMap.keys.foreach { cloneFieldAlias =>
              cloneFieldAlias.cypherType match {
                case _: CTRelationship if !newPattern.fields.contains(cloneFieldAlias) =>
                  throw UnsupportedOperationException(s"Can only clone relationship ${cloneFieldAlias.name} if it is also part of a NEW pattern")
                case _ => ()
              }
            }

            val fieldsInNewPattern = newPattern
              .fields
              .filterNot(cloneItemMap.contains)

            val constructOperatorSchema = fieldsInNewPattern.foldLeft(cloneSchema) { case (agg, next) =>
              val actualLabelsOrTypes = labelsOrTypesFromNewPattern(next, newPattern, context)
              val actualProperties = propertyKeysFromNewPattern(next, newPattern, context)

              next.cypherType match {
                case n: CTNode =>
                  agg.withNodePropertyKeys(actualLabelsOrTypes, actualProperties)

                case r: CTRelationship =>
                  actualLabelsOrTypes.foldLeft(agg) {
                    case (agg2, typ) => agg2.withRelationshipPropertyKeys(typ, actualProperties)
                  }

                case other => throw IllegalArgumentException("A node or a relationship", other)
              }
            }

            val patternGraphSchema = schemaForOnGraphUnion ++ constructOperatorSchema

            val patternGraph = IRPatternGraph[Expr](
              qgn,
              patternGraphSchema,
              cloneItemMap,
              newPattern,
              onGraphs)
            val updatedContext = context.withWorkingGraph(patternGraph).registerSchema(qgn, patternGraphSchema)
            put[R, IRBuilderContext](updatedContext) >> pure[R, List[Block[Expr]]](List.empty)
          }
        } yield refs

      case ast.ReturnGraph(None) =>
        for {
          context <- get[R, IRBuilderContext]
          refs <- {
            val after = context.blockRegistry.lastAdded.toList
            val returns = GraphResultBlock[Expr](after, context.workingGraph)
            val updatedRegistry = context.blockRegistry.register(returns)
            put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry)) >> pure[R, List[Block[Expr]]](List(returns))
          }
        } yield refs

      case ast.Return(distinct, ast.ReturnItems(_, items), orderBy, skip, limit, _) =>
        for {
          fieldExprs <- items.toList.traverse(convertReturnItem[R])
          context <- get[R, IRBuilderContext]
          blocks1 <- {
            val (projectRef, projectReg) =
              registerProjectBlock(context, fieldExprs, distinct = distinct, source = context.workingGraph)
            val appendList = (list: List[Block[Expr]]) => pure[R, List[Block[Expr]]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R, IRBuilderContext](context.copy(blockRegistry = projectReg)) >> orderAndSliceBlock flatMap appendList
          }
          context2 <- get[R, IRBuilderContext]
          blocks2 <- {
            val rItems = fieldExprs.map(_._1)
            val orderedFields = OrderedFields[Expr](rItems)
            val resultBlock = TableResultBlock[Expr](List(blocks1.last), orderedFields, context.workingGraph)
            val updatedRegistry = context2.blockRegistry.register(resultBlock)
            put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry)) >> pure[R, List[Block[Expr]]](blocks1 :+ resultBlock)
          }
        } yield blocks2

      case x =>
        error(IRBuilderError(s"Clause not yet supported: $x"))(List.empty[Block[Expr]])
    }
  }

  def schemaForEntityTypes( context: IRBuilderContext, cypherTypes: Set[CypherType]): Schema =
    cypherTypes
      .map( schemaForEntityType(context, _) )
      .foldLeft(Schema.empty)(_ ++ _)

  def schemaForEntityType(context: IRBuilderContext, cypherType: CypherType): Schema = {
    val graphSchema = cypherType.graph.map(context.schemaFor).getOrElse(context.workingGraph.schema)
    graphSchema.forEntityType(cypherType)
  }

  private def registerProjectBlock(
    context: IRBuilderContext,
    fieldExprs: List[(IRField, Expr)],
    given: Set[Expr] = Set.empty[Expr],
    source: IRGraph,
    distinct: Boolean
  ): (Block[Expr], BlockRegistry[Expr]) = {
    val blockRegistry = context.blockRegistry
    val binds = Fields(fieldExprs.toMap)

    val after = blockRegistry.lastAdded.toList
    val projs = ProjectBlock[Expr](after, binds, given, source, distinct)

    projs -> blockRegistry.register(projs)
  }

  private def registerOrderAndSliceBlock[R: _mayFail : _hasContext](
    orderBy: Option[ast.OrderBy],
    skip: Option[ast.Skip],
    limit: Option[ast.Limit]
  ) = {
    for {
      context <- get[R, IRBuilderContext]
      sortItems <- orderBy match {
        case Some(ast.OrderBy(sortItems)) =>
          sortItems.toList.traverse(convertSortItem[R])
        case None => List[ast.SortItem]().traverse(convertSortItem[R])
      }
      skipExpr <- convertExpr(skip.map(_.expression))
      limitExpr <- convertExpr(limit.map(_.expression))

      blocks <- {
        if (sortItems.isEmpty && skipExpr.isEmpty && limitExpr.isEmpty) pure[R, List[Block[Expr]]](List())
        else {
          val blockRegistry = context.blockRegistry
          val after = blockRegistry.lastAdded.toList

          val orderAndSliceBlock = OrderAndSliceBlock[Expr](after, sortItems, skipExpr, limitExpr, context.workingGraph)
          val updatedRegistry = blockRegistry.register(orderAndSliceBlock)
          put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry)) >> pure[R, List[Block[Expr]]](List(orderAndSliceBlock))
        }
      }
    } yield blocks
  }

  private def convertClone[R: _mayFail : _hasContext](
    item: ast.ReturnItem,
    qgn: QualifiedGraphName
  ): Eff[R, (IRField, Expr)] = {

    def convert(cypherType: CypherType, name: String): IRField = IRField(name)(cypherType.withGraph(qgn))

    item match {

      case ast.AliasedReturnItem(e, v) =>
        for {
          expr <- convertExpr(e)
          context <- get[R, IRBuilderContext]
          field <- {
            val field = convert(expr.cypherType, v.name)
            put[R, IRBuilderContext](context.withFields(Set(field))) >> pure[R, IRField](field)
          }
        } yield field -> expr

      case ast.UnaliasedReturnItem(e, name) =>
        for {
          expr <- convertExpr(e)
          context <- get[R, IRBuilderContext]
          field <- {
            val field = convert(expr.cypherType, name)
            put[R, IRBuilderContext](context.withFields(Set(field))) >> pure[R, IRField](field)
          }
        } yield field -> expr

      case _ =>
        throw IllegalArgumentException(s"${ast.AliasedReturnItem.getClass} or ${ast.UnaliasedReturnItem.getClass}", item.getClass)
    }
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
    variable: exp.Variable
  ): Eff[R, (Expr, IRField)] = {
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

  private def convertPattern[R: _hasContext](
    p: exp.Pattern,
    qgn: Option[QualifiedGraphName] = None
  ): Eff[R, Pattern[Expr]] = {
    for {
      context <- get[R, IRBuilderContext]
      result <- {
        val pattern = context.convertPattern(p, qgn)
        val patternTypes = pattern.fields.foldLeft(context.knownTypes) {
          case (acc, f) => acc.updated(exp.Variable(f.name)(InputPosition.NONE), f.cypherType)
        }
        put[R, IRBuilderContext](context.copy(knownTypes = patternTypes)) >> pure[R, Pattern[Expr]](pattern)
      }
    } yield result
  }

  private def convertSetItem[R: _hasContext](p: ast.SetItem): Eff[R, SetItem[Expr]] = {
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

  private def convertExpr[R: _hasContext](e: exp.Expression): Eff[R, Expr] =
    for {
      context <- get[R, IRBuilderContext]
    } yield context.convertExpression(e)

  private def convertWhere[R: _hasContext](where: Option[ast.Where]): Eff[R, Set[Expr]] = where match {
    case Some(ast.Where(expr)) =>
      for {
        predicate <- convertExpr(expr)
      } yield {
        predicate match {
          case org.opencypher.okapi.ir.api.expr.Ands(exprs) => exprs.toSet
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
      val blocks = context.blockRegistry
      val model = QueryModel(blocks.lastAdded.get.asInstanceOf[ResultBlock[Expr]], context.parameters)
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

  private def labelsOrTypesFromNewPattern(field: IRField, pattern: Pattern[Expr], context: IRBuilderContext): Set[String] = {
    val baseFieldLabelsOrTypes: Set[String] = pattern.baseFields.get(field) match {
      case Some(baseEntity) => baseEntity.cypherType match {
        case CTNode(labels, _) => labels
        case CTRelationship(types, _) => types
        case _ => ???
      }
      case None => Set.empty
    }

    field.cypherType match {
      case n: CTNode => n.labels ++ baseFieldLabelsOrTypes
      case r: CTRelationship => if (r.types.nonEmpty) r.types else baseFieldLabelsOrTypes
      case other => throw IllegalArgumentException("A node or a relationship", other)
    }
  }

  private def propertyKeysFromNewPattern(field: IRField, pattern: Pattern[Expr], context: IRBuilderContext ) = {
    val baseFieldProperties = pattern.baseFields.get(field) match {
      case Some(baseNode) =>
        val baseEntitySchema = schemaForEntityType(context, baseNode.cypherType)

        baseNode.cypherType match {
          case ct: CTNode => baseEntitySchema.nodeKeys(ct.labels.toSeq: _*)
          case ct: CTRelationship => ct.types.map(baseEntitySchema.relationshipKeys).reduce(_ ++ _)
          case other => throw IllegalArgumentException(s"Base Entity type cannot be of type $other")
        }

      case _ =>
        PropertyKeys.empty
    }

    val newProperties = extractPropertyKeysFromIRField(field, pattern.properties)
    baseFieldProperties ++ newProperties
  }

  private def extractPropertyKeysFromIRField(
    f: IRField,
    irFieldProperties: Map[IRField, MapExpression]
  ): PropertyKeys = {
    irFieldProperties.get(f) match {
      case None => PropertyKeys.empty
      case Some(MapExpression(items)) =>
        items.map { case (key, expr) =>
          key -> expr.cypherType
        }
    }
  }
}
