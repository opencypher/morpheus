package org.opencypher.spark.api.spark

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.graph.CypherGraph
import org.opencypher.spark.api.record.{OpaqueField, RecordHeader}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTNode, CTRelationship, CypherType}

trait SparkCypherGraph extends CypherGraph {

  self =>

  override type Space = SparkGraphSpace
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords
}

object SparkCypherGraph {

  def empty(graphSpace: SparkGraphSpace): SparkCypherGraph =
    EmptyGraph(graphSpace, SparkCypherRecords.empty()(graphSpace))

  private sealed case class EmptyGraph(
    graphSpace: SparkGraphSpace,
    override val details: SparkCypherRecords
  ) extends SparkCypherGraph {

    override def nodes(v: Var): SparkCypherRecords =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(v)))(graphSpace)

    override def relationships(v: Var): SparkCypherRecords =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(v)))(graphSpace)

    override def space = graphSpace
    override def schema = Schema.empty
  }
}
