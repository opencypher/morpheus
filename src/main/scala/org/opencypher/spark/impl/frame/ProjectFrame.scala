package org.opencypher.spark.impl.frame

import org.opencypher.spark.impl.{StdCypherFrame, StdField, StdFrameSignature}

abstract class ProjectFrame(val projectedField: StdField, signature: StdFrameSignature) extends StdCypherFrame[Product](signature)
