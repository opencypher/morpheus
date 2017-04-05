package org.opencypher.spark.prototype.impl.typer

case class TyperResult[A](value: A, context: TyperContext)
