package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.spark.DataFrameGraphBuilder
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.impl.util.{Verifiable, Verified}

import scala.language.implicitConversions

sealed trait EntityDataFrame {

  type Self <: EntityDataFrame

  def df: DataFrame
  def id: Int
  def propertyInfo: Map[String, (Int, CypherType)]

  def withProperty(name: String, column: Int, cypherType: CypherType): Self
}

final case class NodeDataFrame(
  df: DataFrame,
  id: Int,
  labelInfo: Map[String, Int],
  propertyInfo: Map[String, (Int, CypherType)]
) extends EntityDataFrame {

  self =>

  override type Self = NodeDataFrame

  def withLabel(name: String, column: Int) =
    copy(labelInfo = labelInfo.updated(name, column))

  def withProperty(name: String, column: Int, cypherType: CypherType) =
    copy(propertyInfo = propertyInfo.updated(name, column -> cypherType))
}

final case class RelationshipDataFrame(
  df: DataFrame,
  ids: (Int, Int, Int),
  relInfo: Either[String, Int],
  propertyInfo: Map[String, (Int, CypherType)]
) extends EntityDataFrame {

  override type Self = RelationshipDataFrame

  override def id = ids._2

  def withProperty(name: String, column: Int, cypherType: CypherType) =
    copy(propertyInfo = propertyInfo.updated(name, column -> cypherType))
}

object DataFrameGraph {
  val empty = DataFrameGraphBuilder.build

  implicit def verifyDataFrameGraph(graph: DataFrameGraph): VerifiedDataFrameGraph = graph.verify
}

final case class DataFrameGraph(nodeDf: Option[NodeDataFrame], relDf: Option[RelationshipDataFrame])
extends Verifiable {

  self =>

  override type Self = DataFrameGraph
  override type VerifiedSelf = VerifiedDataFrameGraph

  override def verify: VerifiedDataFrameGraph = new VerifiedDataFrameGraph {
    override def verified: DataFrameGraph = self
  }
}

sealed trait VerifiedDataFrameGraph extends Verified[DataFrameGraph] {
  final def entities = verified
}
