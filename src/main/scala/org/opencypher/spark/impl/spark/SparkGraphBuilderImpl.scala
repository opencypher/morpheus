package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.spark._
import org.opencypher.spark.api.types.CypherType

// TODO: Schema
// TODO: Verification of properties and labels (no conflicts with previous declarations, matching types, columns)
// TODO: Check that all start nodes and end nodes exist
// TODO: Use maps instead of sets?
//
class SparkGraphBuilderImpl(entities: ExternalGraph) extends DescribeExternalGraph {

  override def withNodesDF(df: DataFrame, id: Int): DescribeExternalNodes = entities.nodeFrame match {
    case Some(_) =>
      throw new UnsupportedOperationException("Multiple node frame inputs are not yet supported")

    case _ =>
      val nodeFrame = ExternalNodes(df, id, Set.empty, Set.empty)
      SparkNodesMapperImpl(nodeFrame, done.relFrame)
  }

  override def withRelationshipsDF(df: DataFrame, ids: (Int, Int, Int)): DescribeExternalRelationships = ???

  override def done: ExternalGraph = entities

  private case class SparkNodesMapperImpl(nodes: ExternalNodes, optRels: Option[VerifiedExternalRelationships])
    extends DescribeExternalNodes {

    override def label(name: String, column: Int): DescribeExternalNodes =
      copy(nodes = nodes.withLabel(name, column))

    override def property(name: String, column: Int, cypherType: CypherType): DescribeExternalNodes =
      copy(nodes = nodes.withProperty(name, column, cypherType))

    override def propertiesAsGiven: DescribeExternalGraph = ???

    override def and: DescribeExternalGraph =
      new SparkGraphBuilderImpl(ExternalGraph(Some(nodes.verify), optRels))
  }
}
