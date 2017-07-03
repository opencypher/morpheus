package org.opencypher.spark.api.spark

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.graph.CypherGraph
import org.opencypher.spark.api.record.{OpaqueField, RecordHeader}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTNode, CTRelationship}

trait SparkCypherGraph extends CypherGraph {

  self =>

  override type Space = SparkGraphSpace
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords
}

object SparkCypherGraph {

  def empty(graphSpace: SparkGraphSpace): SparkCypherGraph =
    EmptyGraph(graphSpace)

  private sealed case class EmptyGraph(
    graphSpace: SparkGraphSpace
  ) extends SparkCypherGraph {

    override def nodes(name: String): SparkCypherRecords =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(CTNode))))(graphSpace)

    override def relationships(name: String): SparkCypherRecords =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(CTRelationship))))(graphSpace)

    override def space = graphSpace
    override def schema = Schema.empty
  }
}
