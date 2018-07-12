package org.opencypher.okapi.relational.api.graph

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}

trait RelationalCypherGraph[T <: FlatRelationalTable[T]] extends PropertyGraph {

  type Graph <: RelationalCypherGraph[T]

  def tags: Set[Int]

  def cache(): Graph

  override def nodes(name: String, nodeCypherType: CTNode = CTNode): RelationalCypherRecords[T]

  override def relationships(name: String, relCypherType: CTRelationship = CTRelationship): RelationalCypherRecords[T]
}
