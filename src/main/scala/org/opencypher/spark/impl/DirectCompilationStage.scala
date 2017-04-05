package org.opencypher.spark.impl

trait DirectCompilationStage[-A, B, C] extends CompilationStage[A, B, C] {
  final override type Out = B
  final override def extract(output: Out): B = output
}
