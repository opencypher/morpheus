package org.opencypher.spark.impl.frame

import org.opencypher.spark.impl.{StdCypherFrame, StdField, StdFrameSignature}

abstract class ProjectFrame(signature: StdFrameSignature) extends StdCypherFrame[Product](signature) {
  def projectedField: StdField

}
