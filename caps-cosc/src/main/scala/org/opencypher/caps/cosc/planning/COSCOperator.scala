package org.opencypher.caps.cosc.planning

import java.net.URI

import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.cosc.COSCConverters._
import org.opencypher.caps.cosc._
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.ir.api.expr.Var
import org.opencypher.caps.logical.impl.{LogicalExternalGraph, LogicalGraph}
import org.opencypher.caps.trees.AbstractTreeNode

abstract class COSCOperator extends AbstractTreeNode[COSCOperator] {

  def execute(implicit context: COSCRuntimeContext): COSCPhysicalResult

  protected def resolve(uri: URI)(implicit context: COSCRuntimeContext): COSCGraph = {
    context.resolve(uri).map(_.asCosc).getOrElse(throw IllegalArgumentException(s"a graph at $uri"))
  }
}

case class COSCScan(in: COSCOperator, inGraph: LogicalGraph, v: Var, header: RecordHeader) extends COSCOperator {

  override def execute(implicit context: COSCRuntimeContext): COSCPhysicalResult = {
    val graphs = in.execute.graphs
    val graph = graphs(inGraph.name)
    val records = v.cypherType match {
      case r: CTRelationship =>
        graph.relationships(v.name, r)
      case n: CTNode =>
        graph.nodes(v.name, n)
      case x =>
        throw IllegalArgumentException("an entity type", x)
    }
    assert(header == records.header)
    COSCPhysicalResult(records, graphs)
  }
}


case class COSCStart(records: COSCRecords, graph: LogicalExternalGraph) extends COSCOperator {
  override def execute(implicit context: COSCRuntimeContext): COSCPhysicalResult =
    COSCPhysicalResult(records, Map(graph.name -> resolve(graph.uri)))
}

case class COSCSetSourceGraph(in: COSCOperator, graph: LogicalExternalGraph) extends COSCOperator {
  override def execute(implicit context: COSCRuntimeContext): COSCPhysicalResult =
    in.execute.withGraph(graph.name -> resolve(graph.uri))
}
