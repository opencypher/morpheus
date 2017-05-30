package org.opencypher.spark.impl.classes

import org.opencypher.spark.api.graph.{CypherGraph, CypherResult, GraphSpace}
import org.opencypher.spark.api.record.CypherRecords
import org.opencypher.spark.api.value.CypherValue

trait Cypher {

  self =>

  type Graph <: CypherGraph { type Space = self.Space; type Graph = self.Graph; type Records = self.Records }
  type Space <: GraphSpace { type Graph = self.Graph }
  type Records <: CypherRecords { type Records = self.Records; type Data = self.Data }
  type Result <: CypherResult { type Graph = self.Graph; type Records = self.Records; type Result = self.Result }
  type Data

  final def cypher(graph: Graph, query: String): Result =
    cypher(graph, query, Map.empty)

  def cypher(graph: Graph, query: String, parameters: Map[String, CypherValue]): Result
}
