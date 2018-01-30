package org.opencypher.caps.impl.spark.physical.operators

trait InheritedHeader {
  this: PhysicalOperator =>
    override val header = children.head.header
}
