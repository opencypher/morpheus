package org.opencypher.spark.impl.graph

import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr.{Expr, Property, Var}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable

object ToRefactor {

  // TODO: move this to okapi-relational and plan as operators
  def nodesWithExactLabels(graph: RelationalCypherGraph[DataFrameTable], name: String, labels: Set[String]): CAPSRecords = {
    val nodeType = CTNode(labels)
    val nodeVar = Var(name)(nodeType)
    val records = graph.nodes(name, nodeType, exactLabelMatch = false)

    val header = records.header

    val labelExprs = header.labelsFor(nodeVar)

    val propertyExprs = graph.schema.nodeKeys(labels).flatMap {
      case (key, cypherType) => Property(nodeVar, PropertyKey(key))(cypherType)
    }.toSet
    val headerPropertyExprs = header.propertiesFor(nodeVar).filter(propertyExprs.contains)

    val keepExprs: Seq[Expr] = Seq(nodeVar) ++ labelExprs ++ headerPropertyExprs

    val keepColumns = keepExprs.map(header.column)

    // we only keep rows where all "other" labels are false
    val predicate = labelExprs
      .filterNot(l => labels.contains(l.label.name))
      .map(header.column)
      .map(records.table.df.col(_) === false)
      .reduceOption(_ && _)

    // filter rows and select only necessary columns
    val updatedData = predicate match {

      case Some(filter) =>
        records.table.df
          .filter(filter)
          .select(keepColumns.head, keepColumns.tail: _*)

      case None =>
        records.table.df.select(keepColumns.head, keepColumns.tail: _*)
    }

    val updatedHeader = RecordHeader.from(keepExprs)

    CAPSRecords(updatedHeader, updatedData)
  }

}
