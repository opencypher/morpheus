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

import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState
import org.neo4j.cypher.internal.util.v3_4.{InputPosition, Ref}
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.opencypher.okapi.api.graph.{Namespace, QualifiedGraphName}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.ir.api.block.SourceBlock
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.api.pattern.Pattern
import org.opencypher.okapi.ir.api.{IRCatalogGraph, IRField, IRGraph}
import org.opencypher.okapi.ir.impl.typer.exception.TypingException
import org.opencypher.okapi.ir.impl.typer.{SchemaTyper, TypeTracker}

final case class IRBuilderContext(
  uniqueSessionGraphNameGenerator: () => QualifiedGraphName,
  queryString: String,
  parameters: CypherMap,
  workingGraph: IRGraph, // initially the ambient graph, but gets changed by `USE GRAPH`/`CONSTRUCT`
  blockRegistry: BlockRegistry[Expr] = BlockRegistry.empty[Expr],
  semanticState: SemanticState,
  resolver: Namespace => PropertyGraphDataSource,
  knownTypes: Map[ast.Expression, CypherType] = Map.empty) {
  self =>

  private lazy val exprConverter = new ExpressionConverter()(self)
  private lazy val patternConverter = new PatternConverter

  def convertPattern(p: ast.Pattern): Pattern[Expr] = {
    patternConverter.convert(p, knownTypes, workingGraph.qualifiedGraphName)
  }

  def convertExpression(e: ast.Expression): Expr = {
    val inferred = infer(e)
    val convert = exprConverter.convert(e)(inferred)
    convert
  }

  // TODO: Fuse monads
  def infer(expr: ast.Expression): Map[Ref[ast.Expression], CypherType] = {
    typer.infer(expr, TypeTracker(List(knownTypes), parameters.value.mapValues(_.cypherType))) match {
      case Right(result) =>
        result.recorder.toMap

      case Left(errors) =>
        throw TypingException(s"Type inference errors: ${errors.toList.mkString(", ")}")
    }
  }

  private def typer = SchemaTyper(workingGraph.schema)

  def schemaFor(qualifiedGraphName: QualifiedGraphName): Schema = {
    val dataSource = resolver(qualifiedGraphName.namespace)

    dataSource.schema(qualifiedGraphName.graphName) match {
      case None =>
        // This initialises the graph eagerly!!
        // TODO: We probably want to save the graph reference somewhere
        dataSource.graph(qualifiedGraphName.graphName).schema
      case Some(s) => s
    }
  }

  def withBlocks(reg: BlockRegistry[Expr]): IRBuilderContext = copy(blockRegistry = reg)

  def withFields(fields: Set[IRField]): IRBuilderContext = {
    val withFieldTypes = fields.foldLeft(knownTypes) {
      case (acc, f) =>
        acc.updated(ast.Variable(f.name)(InputPosition.NONE), f.cypherType)
    }
    copy(knownTypes = withFieldTypes)
  }

  def withWorkingGraph(graph: IRGraph): IRBuilderContext =
    copy(workingGraph = graph)
}

object IRBuilderContext {

  def initial(
    query: String,
    parameters: CypherMap,
    semState: SemanticState,
    workingGraph: IRCatalogGraph,
    uniqueSessionGraphNameGenerator: () => QualifiedGraphName,
    resolver: Namespace => PropertyGraphDataSource,
    fieldsFromDrivingTable: Set[Var] = Set.empty
  ): IRBuilderContext = {
    val registry = BlockRegistry.empty[Expr]
    val block = SourceBlock[Expr](workingGraph)
    val updatedRegistry = registry.register(block)

    val context = IRBuilderContext(
      uniqueSessionGraphNameGenerator,
      query,
      parameters,
      workingGraph,
      updatedRegistry,
      semState,
      resolver)

    context.withFields(fieldsFromDrivingTable.map(v => IRField(v.name)(v.cypherType)))
  }
}
