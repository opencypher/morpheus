package org.opencypher.okapi.relational.api.graph

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}

trait RelationalCypherGraph[T <: FlatRelationalTable[T]] extends PropertyGraph {

  def tags: Set[Int]

  override def nodes(name: String, nodeCypherType: CTNode = CTNode): RelationalCypherRecords[T]

  /**
    * Returns all relationships in this graph with the given [[org.opencypher.okapi.api.types.CTRelationship]] type.
    *
    * @param name          field name for the returned relationships
    * @param relCypherType relationship type used for selection
    * @return table of relationships of the specified type
    */
  override def relationships(name: String, relCypherType: CTRelationship = CTRelationship): RelationalCypherRecords[T]
}
