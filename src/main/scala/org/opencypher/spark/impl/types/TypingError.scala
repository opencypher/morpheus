package org.opencypher.spark.impl.types

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression

sealed trait TypingError
case class UntypedExpression(expr: Expression) extends TypingError
