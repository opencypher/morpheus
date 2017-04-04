package org.opencypher.spark.prototype.api.spark

import org.opencypher.spark.api.types.{CTNode, CTRelationship}
import org.opencypher.spark.prototype.api.expr.{Expr, Var}
import org.opencypher.spark.prototype.api.graph.CypherGraph
import org.opencypher.spark.prototype.api.ir.{Field, QueryModel}
import org.opencypher.spark.prototype.api.record.{OpaqueField, RecordHeader}
import org.opencypher.spark.prototype.api.schema.Schema

trait SparkCypherGraph extends CypherGraph {

  self =>

  override type Space = SparkGraphSpace
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords

  def union(other: SparkCypherGraph): SparkCypherGraph = new SparkCypherGraph {

    override def nodes(v: Var) = new SparkCypherGraph {
      override def nodes(v: Var) = self.nodes(v)
      override def relationships(v: Var) = SparkCypherGraph.empty(self.space)
      override def model = QueryModel.nodes(v.name, self.space.globals)
      override def details = self.nodes(v).details.union(other.nodes(v).details)
      override def space = self.space
      override def schema = self.schema
    }

    override def relationships(v: Var) = new SparkCypherGraph {
      override def nodes(v: Var) = self.nodes(v)
      override def relationships(v: Var) = self.relationships(v)
      override def model = QueryModel.relationships(v.name, self.space.globals)
      override def details = self.relationships(v).details.union(other.relationships(v).details)
      override def space = self.space
      override def schema = self.schema
    }

    override def model = ???
    override def details = self.details.union(other.details)

    override def space = self.space
    override def schema = self.schema
  }

  def intersect(other: SparkCypherGraph): SparkCypherGraph = new SparkCypherGraph {

    override def nodes(v: Var) = new SparkCypherGraph {
      override def nodes(v: Var) = self.nodes(v).intersect(other.nodes(v))
      override def relationships(v: Var) = SparkCypherGraph.empty(self.space)
      override def model = QueryModel.nodes(v.name, self.space.globals)
      override def details = self.nodes(v).details.intersect(other.nodes(v).details)
      override def space = self.space
      override def schema = self.schema
    }

    override def relationships(v: Var) = new SparkCypherGraph {
      override def nodes(v: Var) = self.nodes(v).intersect(other.nodes(v))
      override def relationships(v: Var) = self.relationships(v).intersect(other.relationships(v))
      override def model = QueryModel.relationships(v.name, self.space.globals)
      override def details = self.relationships(v).details.intersect(other.relationships(v).details)
      override def space = self.space
      override def schema = self.schema
    }

    override def model = ???
    override def details = self.details.intersect(other.details)

    override def space = self.space
    override def schema = self.schema
  }
}

object SparkCypherGraph {

  def empty(graphSpace: SparkGraphSpace): SparkCypherGraph =
    EmptyGraph(graphSpace, QueryModel.empty[Expr](graphSpace.globals), SparkCypherRecords.empty(graphSpace.session))

  private sealed case class EmptyGraph(
    graphSpace: SparkGraphSpace,
    override val model: QueryModel[Expr],
    override val details: SparkCypherRecords
  ) extends SparkCypherGraph {

    override def nodes(v: Var): SparkCypherGraph = copy(
      model = QueryModel.nodes(v.name, space.globals),
      details = SparkCypherRecords.empty(graphSpace.session, RecordHeader.from(OpaqueField(v, CTNode)))
    )

    override def relationships(v: Var): SparkCypherGraph = copy(
      model = QueryModel.relationships(v.name, space.globals),
      details = SparkCypherRecords.empty(graphSpace.session, RecordHeader.from(OpaqueField(v, CTRelationship)))
    )

    override def space = graphSpace
    override def schema = Schema.empty
  }
}
