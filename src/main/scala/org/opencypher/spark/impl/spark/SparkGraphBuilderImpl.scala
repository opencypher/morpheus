package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.spark._
import org.opencypher.spark.api.types.CypherType

// TODO: Verification: Matching column types
// TODO: Schema
// TODO: Check that all start nodes and end nodes exist
// TODO: Multi-table (?)
// TODO: Build graph
//
class SparkGraphBuilderImpl(entities: ExternalGraph) extends DescribeExternalGraph {

  override def withNodesDF(df: DataFrame, id: Int): DescribeExternalNodes = entities.nodeFrame match {
    case Some(_) =>
      throw new UnsupportedOperationException("Multiple node frame inputs are not yet supported")

    case _ =>
      val nodeFrame = ExternalNodes(df, id, Map.empty, Map.empty)
      SparkNodesMapperImpl(nodeFrame, done.relFrame)
  }

  override def withRelationshipsDF(df: DataFrame, ids: (Int, Int, Int)): DescribeExternalRelationships = ???

  override def done: ExternalGraph = entities

  private case class SparkNodesMapperImpl(nodes: ExternalNodes, optRels: Option[ExternalRelationships])
    extends DescribeExternalNodes {

    override def label(name: String, column: Int): DescribeExternalNodes = {
      if (nodes.labelInfo.contains(name))
        throw new IllegalArgumentException(s"Label :$name has already been declared")
      else
        copy(nodes = nodes.withLabel(name, column))
    }

    override def property(name: String, column: Int, cypherType: CypherType): DescribeExternalNodes = {
      if (nodes.propertyInfo.contains(name))
        throw new IllegalArgumentException(s"Property $name has already been declared")
      else
        copy(nodes = nodes.withProperty(name, column, cypherType))
    }

    override def propertiesAsGiven: DescribeExternalGraph = ???

    override def and: DescribeExternalGraph =
      new SparkGraphBuilderImpl(ExternalGraph(Some(nodes), optRels))
  }
}
