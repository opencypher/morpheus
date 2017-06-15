package org.opencypher.spark.api.graph

import org.opencypher.spark.api.record.CypherRecords

trait GraphSpace {

  self =>

  type Space <: GraphSpace { type Space = self.Space; type Records = self.Records; type Graph = self.Graph }
  type Records <: CypherRecords { type Records = self.Records }
  type Graph <: CypherGraph { type Space = self.Space; type Graph = self.Graph; type Records = self.Records }

  def base: Graph
}


