package org.opencypher.caps.cosc

import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.CypherType._
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.cosc.value.{COSCNode, COSCRelationship}
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.ir.api.expr.Var

object COSCGraph {
  def empty(implicit session: COSCSession): COSCGraph = COSCGraph(Map.empty, Map.empty)(session)

  def create(nodes: Seq[COSCNode], rels: Seq[COSCRelationship])(implicit session: COSCSession): COSCGraph = {
    val labelNodeMap = nodes.groupBy(_.labels)
    val typeRelMap = rels.groupBy(_.relType)
    COSCGraph(labelNodeMap, typeRelMap)
  }
}

case class COSCGraph(labelNodeMap: Map[Set[String], Seq[COSCNode]], typeRelMap: Map[String, Seq[COSCRelationship]])
  (implicit cosc: COSCSession) extends PropertyGraph {

  type CypherSession = COSCSession

  private def allNodes = labelNodeMap.values.flatten.toSeq

  private def allRelationships = typeRelMap.values.flatten.toSeq

  private def schemaForNodes(nodes: Seq[COSCNode], initialSchema: Schema = Schema.empty): Schema =
    nodes.foldLeft(initialSchema) {
      case (tmpSchema, node) =>
        val properties = node.properties.value.map { case (key, value) => key -> value.cypherType }
        tmpSchema.withNodePropertyKeys(node.labels, properties)
    }

  private def schemaForRels(rels: Seq[COSCRelationship], initialSchema: Schema = Schema.empty): Schema =
    rels.foldLeft(initialSchema) {
      case (tmpSchema, rel) =>
        val properties = rel.properties.value.map { case (key, value) => key -> value.cypherType }
        tmpSchema.withRelationshipPropertyKeys(rel.relType, properties)
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
    COSCRecords.create(filteredNodes.map(node => CypherMap(name -> node)).toList, targetNodeHeader)
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
    COSCRecords.create(filteredRels.map(rel => CypherMap(name -> rel)).toList, targetHeader)
  }

  /**
    * Constructs the union of this graph and the argument graph.
    * The argument graph has to be managed by the same session as this graph.
    *
    * @param other the argument graph with which to union.
    * @return the union of this and the argument graph.
    */
  override def union(other: PropertyGraph): PropertyGraph = ???
}
