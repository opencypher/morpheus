package org.opencypher.spark.impl

trait CompilationStage[-A, +B, C] {

  type Out

  final def apply(input: A)(implicit context: C): B =
    extract(process(input))

  def process(input: A)(implicit context: C): Out

  def extract(output: Out): B
}
