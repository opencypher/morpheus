package org.opencypher.okapi.api.schema

object SchemaPattern{
  def apply(
    sourceLabel: String,
    relType: String,
    targetLabel: String
  ): SchemaPattern = new SchemaPattern(Set(sourceLabel), relType, Set(targetLabel))
}

/**
  * Describes a (node)-[relationship]->(node) triple in a graph as part of the graph's schema.
  * A pattern only applies to nodes with the exact label combination defined by `source/targetLabels`
  * and relationships with the specified relationship type.
  *
  * @example Given a graph that only contains the following patterns
  *                        {{{(:A)-[r1:REL]->(:C),}}}
  *                        {{{(:B)-[r2:REL]->(:C),}}}
  *                        {{{(:A:B)-[r3:REL]->(:C)}}}
  *
  *          then the schema pattern `SchemaPattern(Set("A"), "REL", Set("C"))` would only apply to `r1`
  *          and the schema pattern `SchemaPattern(Set("A", "B"), "REL", Set("C"))` would only apply to `r3`
  *
  * @param sourceLabels label combination for source nodes
  * @param relType relationship type
  * @param targetLabels label combination for target nodes
  */
case class SchemaPattern(sourceLabels: Set[String], relType: String, targetLabels: Set[String]) {
  override def toString: String = {
    val sourceLabelString = if(sourceLabels.isEmpty) "" else sourceLabels.mkString(":", ":", "")
    val targetLabelString = if(targetLabels.isEmpty) "" else targetLabels.mkString(":", ":", "")
    s"($sourceLabelString)-[:$relType]->($targetLabelString)"
  }
}