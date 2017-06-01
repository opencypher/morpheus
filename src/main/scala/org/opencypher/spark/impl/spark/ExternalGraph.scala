package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.impl.util.{Verifiable, Verified}

import scala.language.implicitConversions

sealed trait ExternalEntities extends Verifiable {

  override type Self <: ExternalEntities
  override type VerifiedSelf <: VerifiedExternalEntities[Self]

  def df: DataFrame
  def id: Int
  def propertyInfo: Set[(String, Int, CypherType)]

  def withProperty(name: String, column: Int, cypherType: CypherType): Self
}

sealed trait VerifiedExternalEntities[X <: ExternalEntities] extends Verified[X]

final case class ExternalNodes(
  df: DataFrame,
  id: Int,
  labelInfo: Set[(String, Int)],
  propertyInfo: Set[(String, Int, CypherType)]
) extends ExternalEntities {

  override type Self = ExternalNodes
  override type VerifiedSelf = VerifiedExternalNodes

  def withLabel(name: String, column: Int) =
    copy(labelInfo = labelInfo + ((name, column)))

  def withProperty(name: String, column: Int, cypherType: CypherType) =
    copy(propertyInfo = propertyInfo + ((name, column, cypherType)))

  override def verify: VerifiedExternalNodes = ???
}

sealed trait VerifiedExternalNodes extends VerifiedExternalEntities[ExternalNodes]

final case class ExternalRelationships(
  df: DataFrame,
  ids: (Int, Int, Int),
  relInfo: Either[String, Int],
  propertyInfo: Set[(String, Int, CypherType)]
) extends ExternalEntities {

  override type Self = ExternalRelationships
  override type VerifiedSelf = VerifiedExternalRelationships

  override def id = ids._2

  def withProperty(name: String, column: Int, cypherType: CypherType) =
    copy(propertyInfo = propertyInfo + ((name, column, cypherType)))

  override def verify: VerifiedExternalRelationships = ???
}

sealed trait VerifiedExternalRelationships extends VerifiedExternalEntities[ExternalRelationships]

object ExternalGraph {
  implicit def verifyExternalGraph(graph: ExternalGraph): VerifiedExternalGraph = graph.verify
}

final case class ExternalGraph(nodeFrame: Option[VerifiedExternalNodes], relFrame: Option[VerifiedExternalRelationships])
extends Verifiable {
  override type Self = ExternalGraph
  override type VerifiedSelf = VerifiedExternalGraph

  override def verify: VerifiedExternalGraph = ???
}

sealed trait VerifiedExternalGraph extends Verified[ExternalGraph] {
  final def entities = verified
}
