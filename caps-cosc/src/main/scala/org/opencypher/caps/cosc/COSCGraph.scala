package org.opencypher.caps.cosc

import org.opencypher.caps.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.{CAPSNode, CAPSRelationship}
import org.opencypher.caps.impl.record.CypherRecords

object COSCGraph {
  def empty(implicit session: COSCSession): COSCGraph = COSCGraph(Seq.empty, Seq.empty)(session)
}

case class COSCGraph(nodes: Seq[CAPSNode], rels: Seq[CAPSRelationship])(implicit cosc: COSCSession) extends PropertyGraph {
  /**
    * The schema that describes this graph.
    *
    * @return the schema of this graph.
    */
  override def schema: Schema = ???

  /**
    * The session in which this graph is managed.
    *
    * @return the session of this graph.
    */
  override def session: CypherSession = cosc

  /**
    * Constructs a scan table of all the nodes in this graph with the given cypher type.
    *
    * @param name the field name for the returned nodes.
    * @return a table of nodes of the specified type.
    */
  override def nodes(name: String, nodeCypherType: CTNode): CypherRecords = ???

  /**
    * Constructs a scan table of all the relationships in this graph with the given cypher type.
    *
    * @param name the field name for the returned relationships.
    * @return a table of relationships of the specified type.
    */
  override def relationships(name: String, relCypherType: CTRelationship): CypherRecords = ???

  /**
    * Constructs the union of this graph and the argument graph.
    * The argument graph has to be managed by the same session as this graph.
    *
    * @param other the argument graph with which to union.
    * @return the union of this and the argument graph.
    */
  override def union(other: PropertyGraph): PropertyGraph = ???

  override protected def graph: PropertyGraph = this
}
