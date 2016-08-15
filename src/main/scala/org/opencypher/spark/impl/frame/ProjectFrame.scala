package org.opencypher.spark.impl.frame

import org.opencypher.spark.impl.{ProductFrame, StdField, StdFrameSignature}

abstract class ProjectFrame(val projectedField: StdField, sig: StdFrameSignature) extends ProductFrame(sig) {
  def projectedFieldSymbol: Symbol = projectedField.sym
}
