package org.opencypher.spark.impl.graph

import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.operators.{RelationalOperator, Start}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable
import org.opencypher.spark.schema.CAPSSchema

sealed case class EmptyGraph(implicit val caps: CAPSSession) extends RelationalCypherGraph[DataFrameTable] {

  override type Session = CAPSSession

  override type Records = CAPSRecords

  override val schema: CAPSSchema = CAPSSchema.empty

  override def session: CAPSSession = caps

  override def cache(): EmptyGraph = this

  override def tables: Seq[DataFrameTable] = Seq.empty

  override def tags: Set[Int] = Set.empty

  override private[opencypher] def scanOperator(
    entityType: CypherType,
    exactLabelMatch: Boolean
  ): RelationalOperator[DataFrameTable] = {
    Start(caps.records.empty())(session.basicRuntimeContext())
  }
}
