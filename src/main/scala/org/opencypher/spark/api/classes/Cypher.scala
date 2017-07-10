package org.opencypher.spark.api.classes

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.graph.{CypherGraph, CypherResult, GraphSpace}
import org.opencypher.spark.api.record.CypherRecords
import org.opencypher.spark.api.value.CypherValue


trait Cypher {

  self =>

  type Graph <: CypherGraph { type Space = self.Space; type Graph = self.Graph; type Records = self.Records }
  type Space <: GraphSpace { type Space = self.Space; type Graph = self.Graph; type Records = self.Records }
  type Records <: CypherRecords { type Records = self.Records; type Data = self.Data }
  type Result <: CypherResult { type Result = self.Result; type Graph = self.Graph; type Records = self.Records }
  type Data

  final def cypher(graph: Graph, query: String): Result = cypher(graph, query, Map.empty)

  def cypher(graph: Graph, query: String, parameters: Map[String, CypherValue]): Result

  // Auxiliary operators
  def filter(graph: Graph, in: Records, expr: Expr, queryParameters: Map[String, CypherValue]): Records
  def select(graph: Graph, in: Records, fields: IndexedSeq[Var], queryParameters: Map[String, CypherValue]): Records
}


