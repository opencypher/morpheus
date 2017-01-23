package org.opencypher.spark.impl.types

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.opencypher.spark.api.CypherType

object TyperError {
  def show[T <: Expression](it: T) = s"$it [${it.position}]"
}

import org.opencypher.spark.impl.types.TyperError._

sealed trait TyperError

case class UnsupportedExpr(expr: Expression) extends TyperError {
  override def toString = s"Don't know how to type ${show(expr)}"
}

case class UnTypedExpr(it: Expression) extends TyperError {
  override def toString = s"Expected a type for ${show(it)} but found none"
}

case class NoSuitableSignatureForExpr(it: Expression) extends TyperError {
  override def toString = s"Expected a suitable signature for typing ${show(it)} but found none"
}

case class AlreadyTypedExpr(it: Expression, oldTyp: CypherType, newTyp: CypherType) extends TyperError {
  override def toString = s"Tried to type ${show(it)} with $newTyp but it was already typed as $oldTyp"
}

case class InvalidContainerAccess(it: Expression) extends TyperError {
  override def toString = s"Invalid indexing into a container detected when typing ${show(it)}"
}
