/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherString
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block.{SortItem, _}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern.Pattern
import org.opencypher.okapi.ir.api.set.{SetItem, SetLabelItem, SetPropertyItem}
import org.opencypher.okapi.ir.api.util.CompilationStage
import org.opencypher.okapi.ir.impl.exception.ParsingException
import org.opencypher.okapi.ir.impl.refactor.instances._
import org.opencypher.okapi.ir.impl.util.VarConverters.RichIrField
import org.neo4j.cypher.internal.v4_0.ast.QueryPart
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.{ast, expressions => exp}


object IRBuilder extends CompilationStage[ast.Statement, CypherStatement, IRBuilderContext] {

  override type Out = Either[IRBuilderError, (Option[CypherStatement], IRBuilderContext)]

  override def process(input: ast.Statement)(implicit context: IRBuilderContext): Out =
    buildIR[IRBuilderStack[Option[SingleQuery]]](input).run(context)

  def getContext(output: Out): IRBuilderContext = getTuple(output)._2

  private def getTuple(output: Out) =
    output match {
      case Left(error) => throw IllegalStateException(s"Error during IR construction: $error")
      case Right((None, _)) => throw IllegalStateException(s"Failed to construct IR")
      case Right((Some(q), ctx)) => q -> ctx
    }

  override def extract(output: Out): CypherStatement = getTuple(output)._1

  private def buildIR[R: _mayFail : _hasContext](s: ast.Statement): Eff[R, Option[CypherStatement]] =
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
            val maybeSingleQuery = innerQuery.asInstanceOf[Option[SingleQuery]]
            val schema = maybeSingleQuery.get.model.result match {
              case GraphResultBlock(_, irGraph) => context.schemaFor(irGraph.qualifiedGraphName)
              case _ => throw IllegalArgumentException("The query in CATALOG CREATE GRAPH must return a graph")
            }
            val irQgn = QualifiedGraphName(qgn.parts)
            val statement = Some(CreateGraphStatement(IRCatalogGraph(irQgn, schema), maybeSingleQuery.get))
            pure[R, Option[CypherStatement]](statement)
          }
        } yield result

      case ast.CreateView(qgn, params, _, innerQueryString) =>
        for {
          context <- get[R, IRBuilderContext]
          result <- {
            val statement = Some(CreateViewStatement(
              QualifiedGraphName(qgn.parts),
              params.map(_.name).toList,
              innerQueryString
            ))
            pure[R, Option[CypherStatement]](statement)
          }
        } yield result

      case ast.DropView(catalogName) =>
        for {
          context <- get[R, IRBuilderContext]
          result <- {
            val irQgn = QualifiedGraphName(catalogName.parts)
            val statement = Some(DeleteViewStatement(irQgn))
            pure[R, Option[CypherStatement]](statement)
          }
        } yield result

      case ast.DropGraph(catalogName) =>
        for {
          context <- get[R, IRBuilderContext]
          result <- {
            val irQgn = QualifiedGraphName(catalogName.parts)
            val statement = Some(DeleteGraphStatement(irQgn))
            pure[R, Option[CypherStatement]](statement)
          }
        } yield result

      case x =>
        error(IRBuilderError(s"Statement not yet supported: $x"))(None)
    }

  // TODO: return CypherQuery instead of Option[CypherQuery]
  private def convertQueryPart[R: _mayFail : _hasContext](part: ast.QueryPart): Eff[R, Option[CypherQuery]] = {
    part match {
      case ast.SingleQuery(clauses) =>
        val plannedBlocks = for {
          context <- get[R, IRBuilderContext]
          blocks <-  put[R, IRBuilderContext](context.resetRegistry) >> clauses.toList.traverse(convertClause[R])
        } yield blocks
        plannedBlocks >> convertRegistry

      case ast.UnionAll(innerPart, singleQuery) =>
        convertUnion(innerPart, singleQuery, distinct = false)

      case ast.UnionDistinct(innerPart, singleQuery) =>
        convertUnion(innerPart, singleQuery, distinct = true)

      case x =>
        error(IRBuilderError(s"Query not supported: $x"))(None)
    }
  }

  def convertUnion[R: _mayFail : _hasContext](
    innerPart: QueryPart,
    singleQuery: ast.SingleQuery, distinct: Boolean
  ): Eff[R, Option[CypherQuery]] = {
    for {
      first <- convertQueryPart(innerPart)
      second <- convertQueryPart(singleQuery)
    } yield {
      Some(UnionQuery(first.get, second.get, distinct = distinct))
    }
  }

  private def convertClause[R: _mayFail : _hasContext](c: ast.Clause): Eff[R, List[Block]] = {

    c match {

      case v: ast.ViewInvocation =>
        for {
          context <- get[R, IRBuilderContext]
          blocks <- {
            val graph = context.instantiateView(v)
            val generatedQgn = context.qgnGenerator.generate
            val irGraph = IRCatalogGraph(generatedQgn, graph.schema)
            val updatedContext = context.withWorkingGraph(irGraph).registerGraph(generatedQgn, graph)
            put[R, IRBuilderContext](updatedContext) >> pure[R, List[Block]](List.empty)
          }
        } yield blocks

      case ast.GraphByParameter(p: exp.Parameter) =>
        for {
          context <- get[R, IRBuilderContext]
          result <- {
            val maybeParameterValue = context.parameters.get(p.name)
            val fromGraph = maybeParameterValue match {
              case Some(CypherString(paramValue)) => ast.GraphLookup(ast.CatalogName(QualifiedGraphName.splitQgn(paramValue)))(p.position)
              case Some(other) => throw ParsingException(
                s"Parameter ${p.name} needs to be of type ${CTString.toString}, was $other")
              case None => throw ParsingException(s"No parameter ${p.name} was specified ${p.position}")
            }
            convertClause(fromGraph)
          }
        } yield result

      case ast.GraphLookup(qgn: ast.CatalogName) =>
        for {
          context <- get[R, IRBuilderContext]
          blocks <- {
            val irQgn = QualifiedGraphName(qgn.parts)
            val schema = context.schemaFor(irQgn)
            val irGraph = IRCatalogGraph(irQgn, schema)
            val updatedContext = context.withWorkingGraph(irGraph)
            put[R, IRBuilderContext](updatedContext) >> pure[R, List[Block]](List.empty)
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
            val block = MatchBlock(after, pattern, given, optional, context.workingGraph)

            val typedOutputs = typedMatchBlock.outputs(block)
            val updatedRegistry = blockRegistry.register(block)
            val updatedContext = context.withBlocks(updatedRegistry).withFields(typedOutputs)
            put[R, IRBuilderContext](updatedContext) >> pure[R, List[Block]](List(block))
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
            val appendList = (list: List[Block]) => pure[R, List[Block]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R, IRBuilderContext](context.copy(blockRegistry = projectReg)) >> orderAndSliceBlock flatMap appendList
          }
        } yield refs

      case ast.With(_, ast.ReturnItems(_, items), _, _, _, None) =>
        for {
          fieldExprs <- items.toList.traverse(convertReturnItem[R])
          context <- get[R, IRBuilderContext]
          blocks <- {
            val (agg, group) = fieldExprs.partition {
              case (_, _: Aggregator) => true
              case _ => false
            }

            val (projectBlock, updatedRegistry1) = registerProjectBlock(context, group, source = context.workingGraph, distinct = false)
            val after = updatedRegistry1.lastAdded.toList
            val aggregationBlock = AggregationBlock(after, Aggregations(agg.toSet), group.map(_._1).toSet, context.workingGraph)
            val updatedRegistry2 = updatedRegistry1.register(aggregationBlock)

            put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry2)) >> pure[R, List[Block]](List(projectBlock, aggregationBlock))
          }
        } yield blocks

      case ast.Unwind(listExpression, variable) =>
        for {
          tuple <- convertUnwindItem(listExpression, variable)
          context <- get[R, IRBuilderContext]
          block <- {
            val (list, item) = tuple
            val binds: UnwoundList = UnwoundList(list, item)
            val block = UnwindBlock(context.blockRegistry.lastAdded.toList, binds, context.workingGraph)
            val updatedRegistry = context.blockRegistry.register(block)

            put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry)) >> pure[R, List[Block]](List(block))
          }
        } yield block

      case ast.ConstructGraph(clones, creates, on, sets) =>
        for {
          context <- get[R, IRBuilderContext]

          qgn = context.qgnGenerator.generate

          explicitCloneItems <- clones.flatMap(_.items).traverse(convertClone[R](_, qgn))

          createPatterns <- creates.map {
            case ast.CreateInConstruct(p: exp.Pattern) => p
          }.traverse(convertPattern[R](_, Some(qgn)))

          setItems <- sets.flatMap {
            case ast.SetClause(s) => s
          }.traverse(convertSetItem[R])

          refs <- {
            val onGraphs: List[QualifiedGraphName] = on.map(graph => QualifiedGraphName(graph.parts))
            val schemaForOnGraphUnion = onGraphs.foldLeft(Schema.empty) { case (agg, next) =>
              agg ++ context.schemaFor(next)
            }

            // Computing single nodes/rels constructed by CREATEd
            // TODO: Throw exception if both clone alias and original field name are used in CREATE
            val createPattern = createPatterns.foldLeft(Pattern.empty)(_ ++ _)

            // Single nodes/rels constructed by CLONE (MERGE)
            val explicitCloneItemMap = explicitCloneItems.toMap

            // Items from other graphs that are cloned by default
            val implicitCloneItems = createPattern.fields.filterNot { f =>
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
                case _: CTRelationship if !createPattern.fields.contains(cloneFieldAlias) =>
                  throw UnsupportedOperationException(s"Can only clone relationship ${cloneFieldAlias.name} if it is also part of a CREATE pattern")
                case _ => ()
              }
            }

            val fieldsInNewPattern = createPattern
              .fields
              .filterNot(cloneItemMap.contains)

            val patternSchema = fieldsInNewPattern.foldLeft(cloneSchema) { case (acc, next) =>
              val newFieldSchema = schemaForNewField(next, createPattern, context)
              acc ++ newFieldSchema
            }

            val (patternSchemaWithSetItems, _) = setItems.foldLeft(patternSchema -> Map.empty[Var, CypherType]) {
              case ((currentSchema, rewrittenVarTypes), setItem: SetItem) =>
                setItem match {
                  case SetLabelItem(variable, labels) =>
                    val (existingLabels, existingQgn) = rewrittenVarTypes.getOrElse(variable, variable.cypherType) match {
                      case CTNode(ls, qualifiedGraphName) => ls -> qualifiedGraphName
                      case other => throw UnsupportedOperationException(s"SET label on something that is not a node: $other")
                    }
                    val labelsAfterSet = existingLabels ++ labels
                    val updatedSchema = currentSchema.addLabelsToCombo(labels, existingLabels)
                    updatedSchema -> rewrittenVarTypes.updated(variable, CTNode(labelsAfterSet, existingQgn))
                  case SetPropertyItem(propertyKey, variable, setValue) =>
                    val propertyType = setValue.cypherType
                    val updatedSchema = currentSchema.addPropertyToEntity(propertyKey, propertyType, variable.cypherType)
                    updatedSchema -> rewrittenVarTypes
                }
            }

            val patternGraphSchema = schemaForOnGraphUnion ++ patternSchemaWithSetItems

            val patternGraph = IRPatternGraph(
              qgn,
              patternGraphSchema,
              cloneItemMap,
              createPattern,
              setItems,
              onGraphs)
            val updatedContext = context.withWorkingGraph(patternGraph).registerSchema(qgn, patternGraphSchema)
            put[R, IRBuilderContext](updatedContext) >> pure[R, List[Block]](List.empty)
          }
        } yield refs

      case ast.ReturnGraph(None) =>
        for {
          context <- get[R, IRBuilderContext]
          refs <- {
            val after = context.blockRegistry.lastAdded.toList
            val returns = GraphResultBlock(after, context.workingGraph)
            val updatedRegistry = context.blockRegistry.register(returns)
            put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry)) >> pure[R, List[Block]](List(returns))
          }
        } yield refs

      case ast.Return(distinct, ast.ReturnItems(_, items), orderBy, skip, limit, _) =>
        for {
          fieldExprs <- items.toList.traverse(convertReturnItem[R])
          context <- get[R, IRBuilderContext]
          blocks1 <- {
            val (projectRef, projectReg) =
              registerProjectBlock(context, fieldExprs, distinct = distinct, source = context.workingGraph)
            val appendList = (list: List[Block]) => pure[R, List[Block]](projectRef +: list)
            val orderAndSliceBlock = registerOrderAndSliceBlock(orderBy, skip, limit)
            put[R, IRBuilderContext](context.copy(blockRegistry = projectReg)) >> orderAndSliceBlock flatMap appendList
          }
          context2 <- get[R, IRBuilderContext]
          blocks2 <- {
            val rItems = fieldExprs.map(_._1)
            val orderedFields = OrderedFields(rItems)
            val resultBlock = TableResultBlock(List(blocks1.last), orderedFields, context.workingGraph)
            val updatedRegistry = context2.blockRegistry.register(resultBlock)
            put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry)) >> pure[R, List[Block]](blocks1 :+ resultBlock)
          }
        } yield blocks2

      case x =>
        error(IRBuilderError(s"Clause not yet supported: $x"))(List.empty[Block])
    }
  }

  def schemaForEntityTypes(context: IRBuilderContext, cypherTypes: Set[CypherType]): Schema =
    cypherTypes
      .map(schemaForEntityType(context, _))
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
  ): (Block, BlockRegistry) = {
    val blockRegistry = context.blockRegistry
    val binds = Fields(fieldExprs.toMap)

    val after = blockRegistry.lastAdded.toList
    val projs = ProjectBlock(after, binds, given, source, distinct)

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
        if (sortItems.isEmpty && skipExpr.isEmpty && limitExpr.isEmpty) pure[R, List[Block]](List())
        else {
          val blockRegistry = context.blockRegistry
          val after = blockRegistry.lastAdded.toList

          val orderAndSliceBlock = OrderAndSliceBlock(after, sortItems, skipExpr, limitExpr, context.workingGraph)
          val updatedRegistry = blockRegistry.register(orderAndSliceBlock)
          put[R, IRBuilderContext](context.copy(blockRegistry = updatedRegistry)) >> pure[R, List[Block]](List(orderAndSliceBlock))
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
        case CTVoid =>
          pure[R, CypherType](CTNull)
        case x =>
          error(IRBuilderError(s"unwind expression was not a list: $x"))(CTAny)
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
  ): Eff[R, Pattern] = {
    for {
      context <- get[R, IRBuilderContext]
      result <- {
        val pattern = context.convertPattern(p, qgn)
        val patternTypes = pattern.fields.foldLeft(context.knownTypes) {
          case (acc, f) => acc.updated(exp.Variable(f.name)(InputPosition.NONE), f.cypherType)
        }
        put[R, IRBuilderContext](context.copy(knownTypes = patternTypes)) >> pure[R, Pattern](pattern)
      }
    } yield result
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
      pure[R, Set[Expr]](Set.empty)
  }

  private def convertRegistry[R: _mayFail : _hasContext]: Eff[R, Option[CypherQuery]] =
    for {
      context <- get[R, IRBuilderContext]
    } yield {
      val blocks = context.blockRegistry
      val model = QueryModel(blocks.lastAdded.get.asInstanceOf[ResultBlock], context.parameters)

      Some(SingleQuery(model))
    }

  private def convertSortItem[R: _mayFail : _hasContext](item: ast.SortItem): Eff[R, SortItem] = {
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

  private def schemaForNewField(field: IRField, pattern: Pattern, context: IRBuilderContext): Schema = {
    val baseFieldSchema = pattern.baseFields.get(field).map { baseNode =>
      schemaForEntityType(context, baseNode.cypherType)
    }.getOrElse(Schema.empty)

    val newPropertyKeys: Map[String, CypherType] = pattern.properties.get(field)
      .map(_.items.map(p => p._1 -> p._2.cypherType))
      .getOrElse(Map.empty)

    field.cypherType match {
      case CTNode(newLabels, _) =>
        val oldLabelCombosToNewLabelCombos = if (baseFieldSchema.labels.nonEmpty)
          baseFieldSchema.allCombinations.map(oldLabels => oldLabels -> (oldLabels ++ newLabels))
        else
          Set(Set.empty[String] -> newLabels)

        val updatedPropertyKeys = oldLabelCombosToNewLabelCombos.map {
          case (oldLabelCombo, newLabelCombo) => newLabelCombo -> (baseFieldSchema.nodePropertyKeys(oldLabelCombo) ++ newPropertyKeys)
        }

        updatedPropertyKeys.foldLeft(Schema.empty) {
          case (acc, (labelCombo, propertyKeys)) => acc.withNodePropertyKeys(labelCombo, propertyKeys)
        }

      // if there is only one relationship type we need to merge all existing types and update them
      case CTRelationship(newTypes, _) if newTypes.size == 1 =>
        val possiblePropertyKeys = baseFieldSchema
          .relTypePropertyMap
          .values
          .map(_.keySet)
          .foldLeft(Set.empty[String])(_ ++ _)

        val joinedPropertyKeys = possiblePropertyKeys.map { key =>
          key -> baseFieldSchema.relationshipPropertyKeyType(Set.empty, key).get
        }.toMap

        val updatedPropertyKeys = joinedPropertyKeys ++ newPropertyKeys

        Schema.empty.withRelationshipPropertyKeys(newTypes.head, updatedPropertyKeys)

      case CTRelationship(newTypes, _) =>
        val actualTypes = if (newTypes.nonEmpty) newTypes else baseFieldSchema.relationshipTypes

        actualTypes.foldLeft(Schema.empty) {
          case (acc, relType) => acc.withRelationshipPropertyKeys(relType, baseFieldSchema.relationshipPropertyKeys(relType) ++ newPropertyKeys)
        }

      case other => throw IllegalArgumentException("CTNode or CTRelationship", other)
    }
  }

  private def convertSetItem[R: _hasContext](p: ast.SetItem): Eff[R, SetItem] = {
    p match {
      case ast.SetPropertyItem(exp.LogicalProperty(map: exp.Variable, exp.PropertyKeyName(propertyName)), setValue: exp.Expression) =>
        for {
          variable <- convertExpr[R](map)
          convertedSetExpr <- convertExpr[R](setValue)
          result <- {
            val setItem = SetPropertyItem(propertyName, variable.asInstanceOf[Var], convertedSetExpr)
            pure[R, SetItem](setItem)
          }
        } yield result
      case ast.SetLabelItem(expr, labels) =>
        for {
          variable <- convertExpr[R](expr)
          result <- {
            val setLabel: SetItem = SetLabelItem(variable.asInstanceOf[Var], labels.map(_.name).toSet)
            pure[R, SetItem](setLabel)
          }
        } yield result
    }
  }

}
