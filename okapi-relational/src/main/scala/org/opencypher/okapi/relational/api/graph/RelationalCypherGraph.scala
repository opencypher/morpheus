package org.opencypher.okapi.relational.api.graph

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.api.tagging.TagSupport._

trait RelationalCypherGraphFactory[T <: FlatRelationalTable[T]] {

  type Graph <: RelationalCypherGraph[T]

  type Records <: RelationalCypherGraphFactory[T]

  def singleTableGraph(records: Records, schema: Schema, tagsUsed: Set[Int]): Graph

  def unionGraph(graphsToReplacements: Map[RelationalCypherGraph[T], Map[Int, Int]]): Graph

  val emptyGraph: Graph

}

trait RelationalCypherGraph[T <: FlatRelationalTable[T]] extends PropertyGraph {

  type Records <: RelationalCypherRecords[T]

  type Session <: RelationalCypherSession[T]

  override def session: Session

  def tags: Set[Int]

  def cache(): RelationalCypherGraph[T]

  override def nodes(name: String, nodeCypherType: CTNode = CTNode, exactLabelMatch: Boolean = false): Records

  override def relationships(name: String, relCypherType: CTRelationship = CTRelationship): Records

  def unionAll(others: PropertyGraph*): RelationalCypherGraph[T] = {
    val graphs = others.map {
      case g: RelationalCypherGraph[T] => g
      case _ => throw UnsupportedOperationException("Union all only works on relational graphs")
    }
    session.graphs.unionGraph(computeRetaggings(graphs.map(g => g -> g.tags).toMap))
  }

}
