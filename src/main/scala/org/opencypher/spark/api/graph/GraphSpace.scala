package org.opencypher.spark.api.graph

import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.record.CypherRecords

// (4) Figure out physical plan
// (5) Execute and flesh out user facing api
trait GraphSpace {

  self =>

  type Space <: GraphSpace { type Space = self.Space; type Records = self.Records; type Graph = self.Graph }
  type Records <: CypherRecords { type Records = self.Records }
  type Graph <: CypherGraph { type Space = self.Space; type Graph = self.Graph; type Records = self.Records }

  def base: Graph
  def globals: GlobalsRegistry

//  def graphs: Set[Graph]
}


