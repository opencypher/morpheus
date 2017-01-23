package org.opencypher.spark.impl.types

case class TyperResult[A](value: A, context: TyperContext)
