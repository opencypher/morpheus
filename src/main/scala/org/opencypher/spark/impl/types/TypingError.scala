package org.opencypher.spark.impl.types

import org.neo4j.cypher.internal.frontend.v3_2.ast.Variable

sealed trait TypingError
case class MissingVariable(v: Variable) extends TypingError
