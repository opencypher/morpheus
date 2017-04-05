package org.opencypher.spark_legacy.impl.frame

import org.opencypher.spark_legacy.impl.{ProductFrame, StdField, StdFrameSignature}

abstract class ProjectFrame(val projectedField: StdField, sig: StdFrameSignature) extends ProductFrame(sig) {
  def projectedFieldSymbol: Symbol = projectedField.sym
}
