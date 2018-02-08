package org.opencypher.caps.cosc.impl.planning

import java.net.URI

import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.physical.PhysicalOperator
import org.opencypher.caps.cosc.impl.COSCConverters._
import org.opencypher.caps.cosc.impl.{COSCGraph, COSCPhysicalResult, COSCRecords, COSCRuntimeContext}
import org.opencypher.caps.trees.AbstractTreeNode

abstract class COSCOperator extends AbstractTreeNode[COSCOperator]
  with PhysicalOperator[COSCRecords, COSCGraph, COSCRuntimeContext] {

  def execute(implicit context: COSCRuntimeContext): COSCPhysicalResult

  protected def resolve(uri: URI)(implicit context: COSCRuntimeContext): COSCGraph = {
    context.resolve(uri).map(_.asCosc).getOrElse(throw IllegalArgumentException(s"a graph at $uri"))
  }
}

