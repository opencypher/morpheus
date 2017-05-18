package org.opencypher.spark.impl.ir

import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, Ref, ast}
import org.opencypher.spark.api.expr.Expr
import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.ir.block.{BlockRef, DefaultGraph, LoadGraphBlock}
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.ir.pattern.Pattern
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.typer.{SchemaTyper, TypeTracker}

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
        throw new IllegalArgumentException(s"Some error in type inference: ${errors.toList.mkString(", ")}")
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

  def withFields(fields: Set[Field]): IRBuilderContext = {
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
