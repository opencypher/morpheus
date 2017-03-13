package org.opencypher.spark.prototype.impl.planner

// TODO: Consider monadifying this using Eff for effects and shapeless for contexts (?)
trait Stage[-A, +B, C] {
  def plan(input: A)(implicit context: C): B
}
