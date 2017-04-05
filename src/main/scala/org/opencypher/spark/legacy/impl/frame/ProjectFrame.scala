package org.opencypher.spark.legacy.impl.frame

import org.opencypher.spark.legacy.impl.{ProductFrame, StdField, StdFrameSignature}

abstract class ProjectFrame(val projectedField: StdField, sig: StdFrameSignature) extends ProductFrame(sig) {
  def projectedFieldSymbol: Symbol = projectedField.sym
}
