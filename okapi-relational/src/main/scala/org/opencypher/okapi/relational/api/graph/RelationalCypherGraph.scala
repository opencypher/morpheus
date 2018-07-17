package org.opencypher.okapi.relational.api.graph

import org.opencypher.okapi.api.graph.{PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.api.tagging.TagSupport._

trait RelationalCypherGraphFactory[T <: FlatRelationalTable[T]] {

  type Graph <: RelationalCypherGraph[T]

  def singleTableGraph(records: RelationalCypherRecords[T], schema: Schema, tagsUsed: Set[Int])
    (implicit context: RelationalRuntimeContext[T]): Graph

  def unionGraph(graphsToReplacements: Map[RelationalCypherGraph[T], Map[Int, Int]])
    (implicit context: RelationalRuntimeContext[T]): Graph

  val empty: Graph

}

trait RelationalCypherGraph[T <: FlatRelationalTable[T]] extends PropertyGraph {

  type Records <: RelationalCypherRecords[T]

  type Session <: RelationalCypherSession[T]

  override def session: Session

  def tags: Set[Int]

  def cache(): RelationalCypherGraph[T] = {
    tables.foreach(_.cache)
    this
  }

  def tables: Seq[T]

  override def nodes(name: String, nodeCypherType: CTNode = CTNode, exactLabelMatch: Boolean = false): Records

  override def relationships(name: String, relCypherType: CTRelationship = CTRelationship): Records

  def unionAll(others: PropertyGraph*): RelationalCypherGraph[T] = {
    val graphs = others.map {
      case g: RelationalCypherGraph[T] => g
      case _ => throw UnsupportedOperationException("Union all only works on relational graphs")
    }

    // TODO: parameterize property graph API with actual graph type to allow for type safe implementations!
    val graphAt = (qgn: QualifiedGraphName) => Some(session.catalog.graph(qgn) match {
      case g: RelationalCypherGraph[_] => g.asInstanceOf[RelationalCypherGraph[T]]
    })

    val context = RelationalRuntimeContext(graphAt)(session)
    session.graphs.unionGraph(computeRetaggings(graphs.map(g => g -> g.tags).toMap))(context)
  }
}
