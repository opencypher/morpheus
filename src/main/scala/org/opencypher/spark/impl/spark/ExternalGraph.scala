package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.impl.util.{Verifiable, Verified}

import scala.language.implicitConversions

sealed trait ExternalEntities {

  type Self <: ExternalEntities

  def df: DataFrame
  def id: Int
  def propertyInfo: Map[String, (Int, CypherType)]

  def withProperty(name: String, column: Int, cypherType: CypherType): Self
}

final case class ExternalNodes(
  df: DataFrame,
  id: Int,
  labelInfo: Map[String, Int],
  propertyInfo: Map[String, (Int, CypherType)]
) extends ExternalEntities {

  self =>

  override type Self = ExternalNodes

  def withLabel(name: String, column: Int) =
    copy(labelInfo = labelInfo.updated(name, column))

  def withProperty(name: String, column: Int, cypherType: CypherType) =
    copy(propertyInfo = propertyInfo.updated(name, column -> cypherType))
}

final case class ExternalRelationships(
  df: DataFrame,
  ids: (Int, Int, Int),
  relInfo: Either[String, Int],
  propertyInfo: Map[String, (Int, CypherType)]
) extends ExternalEntities {

  override type Self = ExternalRelationships

  override def id = ids._2

  def withProperty(name: String, column: Int, cypherType: CypherType) =
    copy(propertyInfo = propertyInfo.updated(name, column -> cypherType))
}

object ExternalGraph {
  implicit def verifyExternalGraph(graph: ExternalGraph): VerifiedExternalGraph = graph.verify
}

final case class ExternalGraph(nodeFrame: Option[ExternalNodes], relFrame: Option[ExternalRelationships])
extends Verifiable {

  self =>

  override type Self = ExternalGraph
  override type VerifiedSelf = VerifiedExternalGraph

  override def verify: VerifiedExternalGraph = new VerifiedExternalGraph {
    override def verified: ExternalGraph = self
  }
}

sealed trait VerifiedExternalGraph extends Verified[ExternalGraph] {
  final def entities = verified
}
