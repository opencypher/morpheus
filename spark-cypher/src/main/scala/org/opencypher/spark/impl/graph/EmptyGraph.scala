package org.opencypher.spark.impl.graph

import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable
import org.opencypher.spark.schema.CAPSSchema

sealed case class EmptyGraph(implicit val caps: CAPSSession) extends RelationalCypherGraph[DataFrameTable] {

  override type Session = CAPSSession

  override type Records = CAPSRecords

  override val schema: CAPSSchema = CAPSSchema.empty

  override def nodes(name: String, cypherType: CTNode, exactLabelMatch: Boolean = false): CAPSRecords =
    caps.records.empty(RecordHeader.from(Var(name)(cypherType)))

  override def relationships(name: String, cypherType: CTRelationship): CAPSRecords =
    caps.records.empty(RecordHeader.from(Var(name)(cypherType)))

  override def session: CAPSSession = caps

  override def cache(): EmptyGraph = this

  override def tags: Set[Int] = Set.empty
}
