package org.opencypher.okapi.relational.api.physical

import org.opencypher.okapi.api.graph.{CypherQueryPlans, CypherResult}
import org.opencypher.okapi.impl.util.PrintOptions
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.operators.RelationalOperator

case class RelationalCypherResult[T <: FlatRelationalTable[T]](
  maybeLogical: Option[LogicalOperator],
  maybeRelational: Option[RelationalOperator[T]]
)(implicit session: RelationalCypherSession[T]) extends CypherResult {

  override type Graph = RelationalCypherGraph[T]

  override def getGraph: Option[Graph] = maybeRelational.map(_.graph)

  override def getRecords: Option[RelationalCypherRecords[T]] = {
    maybeRelational.map(op => session.records.from(op.header, op.table, op.returnItems.map(_.map(_.name))))
  }

  override def records: RelationalCypherRecords[T] = getRecords.get

  override def show(implicit options: PrintOptions): Unit = getRecords match {
    case Some(r) => r.show
    case None => options.stream.print("No results")
  }

  override def plans: QueryPlans[T] = QueryPlans(maybeLogical, maybeRelational)
}

object RelationalCypherResult {

  def empty[T <: FlatRelationalTable[T]](implicit session: RelationalCypherSession[T]): RelationalCypherResult[T] =
    RelationalCypherResult(None, None)

  def apply[T <: FlatRelationalTable[T]](
    logical: LogicalOperator,
    relational: RelationalOperator[T]
  )(implicit session: RelationalCypherSession[T]): RelationalCypherResult[T] =
    RelationalCypherResult(Some(logical), Some(relational))
}
