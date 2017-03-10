package org.opencypher.spark.prototype.impl.classy

import org.opencypher.spark.prototype.api.graph.{CypherGraph, CypherView, GraphSpace}
import org.opencypher.spark.prototype.api.record.CypherRecords
import org.opencypher.spark.prototype.api.value.CypherValue

trait Cypher {

  self =>

  type Graph <: CypherGraph { type Space = self.Space; type View = self.View; type Graph = self.Graph }
  type Space <: GraphSpace { type Graph = self.Graph }
  type View <: CypherView { type Graph = self.Graph; type Records = self.Records }
  type Records <: CypherRecords { type Records = self.Records; type Data = self.Data }
  type Data

  final def cypher(graph: Graph, query: String): View =
    cypher(graph, query, Map.empty)

  def cypher(graph: Graph, query: String, parameters: Map[String, CypherValue]): View
}
