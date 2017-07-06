package org.opencypher.spark.api.graph

import org.opencypher.spark.api.record.CypherRecords
import org.opencypher.spark.api.schema.Schema

trait CypherGraph {

  self =>

  type Space <: GraphSpace { type Graph = self.Graph }
  type Graph <: CypherGraph { type Records = self.Records }
  type Records <: CypherRecords { type Records = self.Records }

  def space: Space

  def schema: Schema

  def nodes(name: String): Records
  def relationships(name: String): Records

}
