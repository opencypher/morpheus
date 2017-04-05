package org.opencypher.spark.prototype.impl.typer

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.opencypher.spark.prototype.api.types._
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

case class InvalidType(it: Expression, expected: CypherType, actual: CypherType) extends TyperError {
  override def toString = s"Expected ${it.show} to be of type $expected, but it was of type $actual"
}
