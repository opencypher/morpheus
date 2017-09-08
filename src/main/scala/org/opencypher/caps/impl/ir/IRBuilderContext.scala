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

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, Ref, ast}
import org.opencypher.caps.api.expr.Expr
import org.opencypher.caps.api.ir.IRField
import org.opencypher.caps.api.ir.block.{BlockRef, DefaultGraph, LoadGraphBlock}
import org.opencypher.caps.api.ir.global.GlobalsRegistry
import org.opencypher.caps.api.ir.pattern.Pattern
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.exception.Raise
import org.opencypher.caps.impl.typer.{SchemaTyper, TypeTracker}

final case class IRBuilderContext(
  queryString: String,
  globals: GlobalsRegistry,
  graphBlock: BlockRef,
  blocks: BlockRegistry[Expr] = BlockRegistry.empty[Expr],
  schemas: Map[BlockRef, Schema],
  knownTypes: Map[ast.Expression, CypherType] = Map.empty)
{
  private lazy val typer = SchemaTyper(schemas(graphBlock))
  private lazy val exprConverter = new ExpressionConverter(globals)
  private lazy val patternConverter = new PatternConverter(globals)

  // TODO: Fuse monads
  def infer(expr: ast.Expression): Map[Ref[ast.Expression], CypherType] = {
    typer.infer(expr, TypeTracker(List(knownTypes))) match {
      case Right(result) =>
        result.recorder.toMap

      case Left(errors) =>
        Raise.typeInferenceFailed(s"${errors.toList.mkString(", ")}")
    }
  }

  def convertPattern(p: ast.Pattern): Pattern[Expr] =
    patternConverter.convert(p)

  def convertExpression(e: ast.Expression): Expr = {
    val inferred = infer(e)
    val convert = exprConverter.convert(e)(inferred)
    convert
  }

  def withBlocks(reg: BlockRegistry[Expr]): IRBuilderContext = copy(blocks = reg)

  def withFields(fields: Set[IRField]): IRBuilderContext = {
    val withFieldTypes = fields.foldLeft(knownTypes) {
      case (acc, f) =>
        acc.updated(ast.Variable(f.name)(InputPosition.NONE), f.cypherType)
    }
    copy(knownTypes = withFieldTypes)
  }

}

object IRBuilderContext {
  def initial(query: String, globals: GlobalsRegistry, schema: Schema, knownTypes: Map[ast.Expression, CypherType]): IRBuilderContext = {
    val registry = BlockRegistry.empty[Expr]

    val block = LoadGraphBlock[Expr](Set.empty, DefaultGraph())
    val (ref, reg) = registry.register(block)

    IRBuilderContext(query, globals, ref, reg, Map(ref -> schema), knownTypes)
  }
}
