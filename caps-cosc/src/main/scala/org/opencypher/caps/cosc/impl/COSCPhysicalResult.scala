package org.opencypher.caps.cosc.impl

case class COSCPhysicalResult(records: COSCRecords, graphs: Map[String, COSCGraph]) {
  def withGraph(t: (String, COSCGraph)): COSCPhysicalResult =
    copy(graphs = graphs.updated(t._1, t._2))
}
