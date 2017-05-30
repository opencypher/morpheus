package org.opencypher.spark.api.spark

import org.opencypher.spark.api.types.{CTNode, CTRelationship}
import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.graph.CypherGraph
import org.opencypher.spark.api.ir.{Field, QueryModel}
import org.opencypher.spark.api.record.{OpaqueField, RecordHeader}
import org.opencypher.spark.api.schema.Schema

trait SparkCypherGraph extends CypherGraph {

  self =>

  override type Space = SparkGraphSpace
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords

}

object SparkCypherGraph {

  def empty(graphSpace: SparkGraphSpace): SparkCypherGraph =
    EmptyGraph(graphSpace, QueryModel.empty[Expr](graphSpace.globals), SparkCypherRecords.empty(graphSpace.session))

  private sealed case class EmptyGraph(
    graphSpace: SparkGraphSpace,
    override val model: QueryModel[Expr],
    override val details: SparkCypherRecords
  ) extends SparkCypherGraph {

    override def nodes(v: Var): SparkCypherRecords = SparkCypherRecords.empty(graphSpace.session, RecordHeader.from(OpaqueField(v)))

    override def relationships(v: Var): SparkCypherRecords = SparkCypherRecords.empty(graphSpace.session, RecordHeader.from(OpaqueField(v)))

    override def space = graphSpace
    override def schema = Schema.empty
  }
}
