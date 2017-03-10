package org.opencypher.spark.prototype.impl.classy

import org.opencypher.spark.prototype.api.graph.{CypherGraph, CypherView, GraphSpace}
import org.opencypher.spark.prototype.api.record.CypherRecords
import org.opencypher.spark.prototype.api.value.CypherValue

trait CypherEngine {
  type EGraph <: CypherGraph { type Space = ESpace; type View = EView; type Graph = EGraph }
  type ESpace <: GraphSpace { type Graph = EGraph }
  type EView <: CypherView { type Graph = EGraph; type Records = ERecords }
  type ERecords <: CypherRecords { type Records = ERecords; type Data = EData }
  type EData

  final def cypher(graph: EGraph, query: String): EView =
    cypher(graph, query, Map.empty)

  def cypher(graph: EGraph, query: String, parameters: Map[String, CypherValue]): EView
}
