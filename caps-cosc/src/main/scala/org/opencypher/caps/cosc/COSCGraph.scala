package org.opencypher.caps.cosc

import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.{CAPSMap, CAPSNode, CAPSRelationship}
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.ir.api.expr.Var

object COSCGraph {
  def empty(implicit session: COSCSession): COSCGraph = COSCGraph(Map.empty, Map.empty)(session)

  def create(nodes: Seq[CAPSNode], rels: Seq[CAPSRelationship])(implicit session: COSCSession): COSCGraph = {
    val labelNodeMap = nodes.groupBy(n => CAPSNode.contents(n) match {
      case Some(content) => content.labels.toSet
      case None => throw IllegalArgumentException("Node with contents")
    })

    val typeRelMap = rels.groupBy(r => CAPSRelationship.contents(r) match {
      case Some(content) => content.relationshipType
      case None => throw IllegalArgumentException("Relationship with contents")
    })

    COSCGraph(labelNodeMap, typeRelMap)
  }
}

case class COSCGraph(labelNodeMap: Map[Set[String], Seq[CAPSNode]], typeRelMap: Map[String, Seq[CAPSRelationship]])
  (implicit cosc: COSCSession) extends PropertyGraph {

  type CypherSession = COSCSession

  private def allNodes = labelNodeMap.values.flatten.toSeq

  private def allRelationships = typeRelMap.values.flatten.toSeq

  private def schemaForNodes(nodes: Seq[CAPSNode], initialSchema: Schema = Schema.empty): Schema =
    nodes.foldLeft(initialSchema) {
      case (tmpSchema, node) => CAPSNode.contents(node) match {
        case Some(content) => tmpSchema.withNodePropertyKeys(content.labels: _*)(
          content.properties.m.keySet.map(key => key -> content.properties(key).cypherType).toSeq: _*)
      }
    }

  private def schemaForRels(rels: Seq[CAPSRelationship], initialSchema: Schema = Schema.empty): Schema =
    rels.foldLeft(initialSchema) {
      case (tmpSchema, rel) => CAPSRelationship.contents(rel) match {
        case Some(content) => tmpSchema.withRelationshipPropertyKeys(content.relationshipType)(
          content.properties.m.keySet.map(key => key -> content.properties(key).cypherType).toSeq: _*)
      }
    }

  /**
    * The schema that describes this graph.
    *
    * @return the schema of this graph.
    */
  override def schema: Schema = schemaForRels(allRelationships, initialSchema = schemaForNodes(allNodes))


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
  override def nodes(name: String, nodeCypherType: CTNode): COSCRecords = {
    val node = Var(name)(nodeCypherType)
    val filteredNodes = if (nodeCypherType.labels.isEmpty) allNodes else labelNodeMap(nodeCypherType.labels)
    val filteredSchema = schemaForNodes(filteredNodes)
    val targetNodeHeader = RecordHeader.nodeFromSchema(node, filteredSchema)
    COSCRecords.create(filteredNodes.map(node => CAPSMap(name -> node)).toList, targetNodeHeader)
  }

  /**
    * Constructs a scan table of all the relationships in this graph with the given cypher type.
    *
    * @param name the field name for the returned relationships.
    * @return a table of relationships of the specified type.
    */
  override def relationships(name: String, relCypherType: CTRelationship): COSCRecords = {
    val rel = Var(name)(relCypherType)
    val filteredRels = if (relCypherType.types.isEmpty) allRelationships else typeRelMap.filterKeys(relCypherType.types.contains).values.flatten.toSeq
    val filteredSchema = schemaForRels(filteredRels)
    val targetHeader = RecordHeader.relationshipFromSchema(rel, filteredSchema)
    COSCRecords.create(filteredRels.map(rel => CAPSMap(name -> rel)).toList, targetHeader)
  }

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
