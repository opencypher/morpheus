package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.CypherNode
import org.opencypher.spark.impl.frame.AllNodes.CypherNodes
import org.opencypher.spark.impl.{StdCypherFrame, StdRuntimeContext}

object LabelFilterNode {
  def apply(input: StdCypherFrame[CypherNode])(labels: Seq[String]): StdCypherFrame[CypherNode] = {
    LabelFilterNode(input)(labels)
  }

  private final case class LabelFilterNode(input: StdCypherFrame[CypherNode])(labels: Seq[String])
    extends StdCypherFrame[CypherNode](input.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[CypherNode] = {
      val in = input.run
      val out = in.filter(labelFilter(labels))
      out
    }
  }

  private final case class labelFilter(labels: Seq[String]) extends (CypherNode => Boolean) {

    override def apply(node: CypherNode): Boolean = {
      labels.forall(node.labels.contains)
    }
  }
}
