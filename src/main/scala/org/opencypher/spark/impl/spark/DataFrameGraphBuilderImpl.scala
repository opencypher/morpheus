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
class DataFrameGraphBuilderImpl(dfGraph: DataFrameGraph) extends DataFrameGraphBuilder {

  override def withNodesDF(df: DataFrame, id: Int): NodeDataFrameBuilder = dfGraph.nodeDf match {
    case Some(_) =>
      throw new UnsupportedOperationException("Multiple node frame inputs are not yet supported")

    case _ =>
      val nodeDf = NodeDataFrame(df, id, Map.empty, Map.empty)
      NodeDataFrameBuilderImpl(nodeDf, build.relDf)
  }

  override def withRelationshipsDF(df: DataFrame, ids: (Int, Int, Int)): RelationshipDataFrameBuilder = ???

  override def build: DataFrameGraph = dfGraph

  private case class NodeDataFrameBuilderImpl(nodeDf: NodeDataFrame, optRelDf: Option[RelationshipDataFrame])
    extends NodeDataFrameBuilder {

    override def label(name: String, column: Int): NodeDataFrameBuilder = {
      if (nodeDf.labelInfo.contains(name))
        throw new IllegalArgumentException(s"Label :$name has already been declared")
      else
        copy(nodeDf = nodeDf.withLabel(name, column))
    }

    override def property(name: String, column: Int, cypherType: CypherType): NodeDataFrameBuilder = {
      if (nodeDf.propertyInfo.contains(name))
        throw new IllegalArgumentException(s"Property $name has already been declared")
      else
        copy(nodeDf = nodeDf.withProperty(name, column, cypherType))
    }

    override def propertiesAsGiven: DataFrameGraphBuilder = ???

    override def and: DataFrameGraphBuilder =
      new DataFrameGraphBuilderImpl(DataFrameGraph(Some(nodeDf), optRelDf))
  }
}
