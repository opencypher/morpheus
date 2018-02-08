package org.opencypher.caps.cosc.impl

import org.opencypher.caps.api.physical.PhysicalResult

case class COSCPhysicalResult(records: COSCRecords, graphs: Map[String, COSCGraph])
  extends PhysicalResult[COSCRecords, COSCGraph] {

  /**
    * Performs the given function on the underlying records and returns the updated records.
    *
    * @param f map function
    * @return updated result
    */
  override def mapRecordsWithDetails(f: COSCRecords => COSCRecords): COSCPhysicalResult =
    copy(records = f(records))

  /**
    * Returns a result that only contains the graphs with the given names.
    *
    * @param names graphs to select
    * @return updated result containing only selected graphs
    */
  override def selectGraphs(names: Set[String]): COSCPhysicalResult =
    copy(graphs = graphs.filterKeys(names))

  /**
    * Stores the given graph identifed by the specified name in the result.
    *
    * @param t tuple mapping a graph name to a graph
    * @return updated result
    */
  override def withGraph(t: (String, COSCGraph)): COSCPhysicalResult =
    copy(graphs = graphs.updated(t._1, t._2))
}
