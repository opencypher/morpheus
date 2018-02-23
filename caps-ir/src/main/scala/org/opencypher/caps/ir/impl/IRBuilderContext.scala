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
package org.opencypher.caps.ir.impl

import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState
import org.neo4j.cypher.internal.util.v3_4.{InputPosition, Ref}
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.opencypher.caps.api.io.{Namespace, PropertyGraphDataSource, QualifiedGraphName}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.CypherType._
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.ir.api.block.SourceBlock
import org.opencypher.caps.ir.api.expr.Expr
import org.opencypher.caps.ir.api.pattern.Pattern
import org.opencypher.caps.ir.api.{IRExternalGraph, IRField, IRGraph}
import org.opencypher.caps.ir.impl.typer.exception.TypingException
import org.opencypher.caps.ir.impl.typer.{SchemaTyper, TypeTracker}

final case class IRBuilderContext(
  queryString: String,
  parameters: CypherMap,
  ambientGraph: IRExternalGraph,
  blocks: BlockRegistry[Expr] = BlockRegistry.empty[Expr],
  semanticState: SemanticState,
  graphs: Map[String, QualifiedGraphName],
  graphList: List[IRGraph],
  resolver: Namespace => PropertyGraphDataSource,
  // TODO: Remove this
  knownTypes: Map[ast.Expression, CypherType] = Map.empty) {
  self =>

  private lazy val patternConverter = new PatternConverter()
  private lazy val exprConverter = new ExpressionConverter(patternConverter)(self)

  def convertPattern(p: ast.Pattern): Pattern[Expr] =
    patternConverter.convert(p, knownTypes)

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

  private def typer = SchemaTyper(currentGraph.schema)

  def currentGraph: IRGraph = graphList.head

  def schemaFor(graphName: String): Schema = {
    val qualifiedGraphName = graphs(graphName)
    val dataSource = resolver(qualifiedGraphName.namespace)

    dataSource.schema(qualifiedGraphName.graphName) match {
      case None =>
        // This initialises the graph eagerly!!
        // TODO: We probably want to save the graph reference somewhere
        dataSource.graph(qualifiedGraphName.graphName).schema
      case Some(s) => s
    }
  }

  def withBlocks(reg: BlockRegistry[Expr]): IRBuilderContext = copy(blocks = reg)

  def withFields(fields: Set[IRField]): IRBuilderContext = {
    val withFieldTypes = fields.foldLeft(knownTypes) {
      case (acc, f) =>
        acc.updated(ast.Variable(f.name)(InputPosition.NONE), f.cypherType)
    }
    copy(knownTypes = withFieldTypes)
  }

  def withGraphAt(name: String, qualifiedName: QualifiedGraphName) =
    copy(graphs = graphs.updated(name, qualifiedName))

  def withGraph(graph: IRGraph): IRBuilderContext =
    copy(graphList = graph :: graphList)
}

object IRBuilderContext {

  def initial(
    query: String,
    parameters: CypherMap,
    semState: SemanticState,
    ambientGraph: IRExternalGraph,
    resolver: Namespace => PropertyGraphDataSource
  ): IRBuilderContext = {
    val registry = BlockRegistry.empty[Expr]
    val block = SourceBlock[Expr](ambientGraph)
    val (_, reg) = registry.register(block)

    IRBuilderContext(
      query,
      parameters,
      ambientGraph,
      reg,
      semState,
      Map(ambientGraph.name -> ambientGraph.qualifiedName),
      List(ambientGraph),
      resolver)
  }
}
