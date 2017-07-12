package org.opencypher.spark.impl.instances.spark

import org.opencypher.spark.impl.instances.ir.block.ExprBlockInstances

trait AllInstances
  extends SparkCypherInstances
  with ExprBlockInstances
