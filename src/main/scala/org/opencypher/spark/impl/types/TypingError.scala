package org.opencypher.spark.impl.types

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, Variable}
import org.opencypher.spark.api.CypherType

sealed trait TypingError
case class UntypedExpression(expr: Expression) extends TypingError
case class InvalidType(expr: Expression, expected: CypherType, actual: CypherType) extends TypingError
