package org.opencypher.spark.prototype.impl

trait PlannerStage[-A, +B, C] {
  def plan(input: A)(implicit context: C): B
}
