package org.opencypher.spark.impl.typer

import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression
import org.opencypher.spark.api.types._
import cats.syntax.show._

sealed trait TyperError

case class UnsupportedExpr(expr: Expression) extends TyperError {
  override def toString = s"Don't know how to type ${expr.show}"
}

case class UnTypedExpr(it: Expression) extends TyperError {
  override def toString = s"Expected a type for ${it.show} but found none"
}

case class NoSuitableSignatureForExpr(it: Expression) extends TyperError {
  override def toString = s"Expected a suitable signature for typing ${it.show} but found none"
}

case class AlreadyTypedExpr(it: Expression, oldTyp: CypherType, newTyp: CypherType) extends TyperError {
  override def toString = s"Tried to type ${it.show} with $newTyp but it was already typed as $oldTyp"
}

case class InvalidContainerAccess(it: Expression) extends TyperError {
  override def toString = s"Invalid indexing into a container detected when typing ${it.show}"
}

object InvalidType {
  def apply(it: Expression, expected: CypherType, actual: CypherType): InvalidType =
    InvalidType(it, Seq(expected), actual)
}

case class InvalidType(it: Expression, expected: Seq[CypherType], actual: CypherType) extends TyperError {
  override def toString = s"Expected ${it.show} to have $expectedString, but it was of type $actual"

  private def expectedString =
    if (expected.size == 1) s"type ${expected.head}"
    else s"one of the types in ${expected.mkString("{ ", ",", " }")}"
}

case object TypeTrackerScopeError extends TyperError {
  override def toString = "Tried to pop scope of type tracker, but it was at top level already"
}
