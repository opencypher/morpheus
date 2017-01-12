package org.opencypher.spark.impl.types

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, Variable}
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.schema.Schema

sealed trait TypingResult {
  def bind(f: TypeContext => TypingResult): TypingResult

  def inferLabels(schema: Schema): TypingResult = ???
}

final case class TypingFailed(errors: Seq[TypingError]) extends TypingResult {
  override def bind(f: TypeContext => TypingResult): TypingResult = this
}

object TypeContext {
  val empty = TypeContext(Map.empty)
}

final case class TypeContext(typeTable: Map[Expression, CypherType]) extends TypingResult {

  override def bind(f: TypeContext => TypingResult): TypingResult = f(this)

  // TODO: Error handling
  def joinType(accType: CypherType, expr: Expression): CypherType = {
    val cypherType = typeTable(expr)
    val foo = accType join cypherType
    foo
  }

  def updateType(update: (Expression, CypherType)) = {
    val (k, v) = update
    copy(typeTable.updated(k, v))
  }

  def variableType(v: Variable)(f: CypherType => TypingResult): TypingResult = typeTable.get(v) match {
    case Some(typ) => f(typ)
    case None      => TypingFailed(Seq(MissingVariable(v)))
  }
}


