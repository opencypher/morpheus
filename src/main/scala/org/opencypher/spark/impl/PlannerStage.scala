package org.opencypher.spark.impl

trait PlannerStage[-A, +B, C] {
  def plan(input: A)(implicit context: C): B
}
