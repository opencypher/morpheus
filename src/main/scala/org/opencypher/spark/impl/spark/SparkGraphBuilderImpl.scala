package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.spark._
import org.opencypher.spark.api.types.{CTNode, CTRelationship, CypherType}

// TODO: Schema
// TODO: Verification of properties and labels (no conflicts with previous declarations, matching types, columns)
// TODO: Check that all start nodes and end nodes exist
// TODO: Use maps instead of sets?
//
class SparkGraphBuilderImpl(graphSpace: SparkGraphSpace, graphInput: GraphEntities = GraphEntities.empty) extends SparkGraphBuilder {

  override def withNodesDF(df: DataFrame, id: Int): SparkNodesMapper = graphInput.nodeFrame match {
    case Some(_) =>
      throw new UnsupportedOperationException("Multiple node frame inputs are not yet supported")

    case _ =>
      val nodes = NodeFrame(df, id, Set.empty, Set.empty)
      SparkNodesMapperImpl(nodes, graphInput.relFrame)
  }

  override def withRelationshipsDF(df: DataFrame, ids: (Int, Int, Int)): SparkRelationshipsMapper = ???

  override def graph: SparkCypherGraph = graphInput.graph(graphSpace)

  private case class SparkNodesMapperImpl(nodes: NodeFrame, optRels: Option[RelationshipFrame]) extends SparkNodesMapper {

    override def label(name: String, column: Int): SparkNodesMapper =
      copy(nodes = nodes.withLabel(name, column))

    override def property(name: String, column: Int, cypherType: CypherType): SparkNodesMapper =
      copy(nodes = nodes.withProperty(name, column, cypherType))

    override def propertiesAsGiven: SparkGraphBuilder = ???

    override def and: SparkGraphBuilder =
      new SparkGraphBuilderImpl(graphSpace, GraphEntities(Some(nodes), optRels))
  }
}


object GraphEntities {
  val empty = GraphEntities(None, None)
}

final case class GraphEntities(nodeFrame: Option[NodeFrame], relFrame: Option[RelationshipFrame]) {

  def graph(graphSpace: SparkGraphSpace): SparkCypherGraph =
    new SparkCypherGraph {

      override def space: SparkGraphSpace = graphSpace

      override def relationships(v: Var): SparkCypherRecords = ???

      override def nodes(v: Var): SparkCypherRecords = ???

      override def schema: Schema = ???

      override def _nodes(name: String, typ: CTNode): SparkCypherRecords = nodeFrame match {
        case Some(input) =>
          ???

        case None =>
          SparkCypherGraph.empty(graphSpace)._nodes(name, typ)
      }

      override def _relationships(name: String, typ: CTRelationship): SparkCypherRecords = relFrame match {
        case Some(input) =>
          ???

        case None =>
          SparkCypherGraph.empty(graphSpace)._relationships(name, typ)
      }
    }
}

