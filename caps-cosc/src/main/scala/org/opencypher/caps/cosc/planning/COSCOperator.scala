package org.opencypher.caps.cosc.planning

import org.opencypher.caps.cosc.{COSCResult, COSCRuntimeContext}
import org.opencypher.caps.trees.AbstractTreeNode

abstract class COSCOperator extends AbstractTreeNode[COSCOperator] {

  def execute(implicit context: COSCRuntimeContext): COSCResult

}
