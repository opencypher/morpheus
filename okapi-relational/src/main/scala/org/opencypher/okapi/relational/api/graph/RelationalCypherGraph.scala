package org.opencypher.okapi.relational.api.graph

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}

trait RelationalCypherGraph[T <: FlatRelationalTable[T]] extends PropertyGraph {

  type Graph <: RelationalCypherGraph[T]

  type Records <: RelationalCypherRecords[T]

  def tags: Set[Int]

  def cache(): Graph

  override def nodes(name: String, nodeCypherType: CTNode = CTNode, exactLabelMatch: Boolean = false): Records

  override def relationships(name: String, relCypherType: CTRelationship = CTRelationship): Records

  /**
    * Constructs the union of this graph and the argument graphs. Note that the argument graphs have to
    * be managed by the same session as this graph.
    *
    * This operation does not merge any nodes or relationships, but simply creates a new graph consisting
    * of all nodes and relationships of the argument graphs.
    *
    * @param others argument graphs with which to union
    * @return union of this and the argument graph
    */
  def unionAll(others: Graph*): RelationalCypherGraph[T] = {
    UnionGraph(this :: others.toList: _*)
  }

//  def nodesWithExactLabels(name: String, labels: Set[String]): RelationalCypherRecords[T] = {
//    val nodeType = CTNode(labels)
//    val nodeVar = Var(name)(nodeType)
//    val records = nodes(name, nodeType)
//
//    val header = records.header
//
//    // compute slot contents to keep
//    val labelExprs = header.labelsFor(nodeVar)
//
//    // need to iterate the slots to maintain the correct order
//    val propertyExprs = schema.nodeKeys(labels).flatMap {
//      case (key, cypherType) => Property(nodeVar, PropertyKey(key))(cypherType)
//    }.toSet
//    val headerPropertyExprs = header.propertiesFor(nodeVar).filter(propertyExprs.contains)
//
//    val keepExprs: Seq[Expr] = Seq(nodeVar) ++ labelExprs ++ headerPropertyExprs
//
//    val keepColumns = keepExprs.map(header.column)
//
//    // we only keep rows where all "other" labels are false
//    val predicate = labelExprs
//      .filterNot(l => labels.contains(l.label.name))
//
//      .map(header.column)
//      .map(records.df.col(_) === false)
//      .reduceOption(_ && _)
//
//    // filter rows and select only necessary columns
//    val updatedData = predicate match {
//
//      case Some(filter) =>
//        records.df
//          .filter(filter)
//          .select(keepColumns.head, keepColumns.tail: _*)
//
//      case None =>
//        records.df.select(keepColumns.head, keepColumns.tail: _*)
//    }
//
//    val updatedHeader = RecordHeader.from(keepExprs)
//
//    CAPSRecords(updatedHeader, updatedData)
//  }
}
