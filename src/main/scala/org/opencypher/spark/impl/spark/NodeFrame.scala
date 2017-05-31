package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.types.CypherType

sealed trait EntityFrame {

  type Self <: EntityFrame

  def df: DataFrame
  def id: Int
  def propertyInfo: Set[(String, Int, CypherType)]

  def withProperty(name: String, column: Int, cypherType: CypherType): Self
}

final case class NodeFrame(
  df: DataFrame,
  id: Int,
  labelInfo: Set[(String, Int)],
  propertyInfo: Set[(String, Int, CypherType)]
) extends EntityFrame {

  override type Self = NodeFrame

  def withLabel(name: String, column: Int) =
    copy(labelInfo = labelInfo + ((name, column)))

  def withProperty(name: String, column: Int, cypherType: CypherType) =
    copy(propertyInfo = propertyInfo + ((name, column, cypherType)))
}

final case class RelationshipFrame(
  df: DataFrame,
  ids: (Int, Int, Int),
  relInfo: Either[String, Int],
  propertyInfo: Set[(String, Int, CypherType)]
) extends EntityFrame {

  override type Self = RelationshipFrame

  override def id = ids._2

  def withProperty(name: String, column: Int, cypherType: CypherType) =
    copy(propertyInfo = propertyInfo + ((name, column, cypherType)))
}

