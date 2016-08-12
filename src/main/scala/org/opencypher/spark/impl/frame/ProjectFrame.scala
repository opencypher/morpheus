package org.opencypher.spark.impl.frame

import org.opencypher.spark.impl.{StdCypherFrame, ProductFrame, StdField, StdFrameSignature}

abstract class ProjectFrame(val projectedField: StdField, sig: StdFrameSignature) extends ProductFrame(sig)
