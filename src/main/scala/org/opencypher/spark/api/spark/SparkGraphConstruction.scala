package org.opencypher.spark.api.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.types._

trait SparkGraphConstruction {

  self: SparkGraphSpace =>

  def fromSpark: SparkGraphBuilder
}

trait SparkGraphBuilder {
  def withNodesDF(df: DataFrame, id: Int): SparkNodesMapper
  def withRelationshipsDF(df: DataFrame, ids: (Int, Int, Int)): SparkRelationshipsMapper
  def graph: SparkCypherGraph
}


trait SparkEntitySource {

  type Self <: SparkEntitySource

  final def graph: SparkCypherGraph = and.graph

  final def property(name: String, column: Int): Self =
    property(name, column, CTAny.nullable)

  def property(name: String, column: Int, cypherType: CypherType): Self

  def propertiesAsGiven: SparkGraphBuilder
  def and: SparkGraphBuilder
}

trait SparkNodesMapper extends SparkEntitySource {

  override type Self = SparkNodesMapper

  // def withCommonLabels(labels: String*): SparkNodesMapper

  def label(name: String, column: Int): SparkNodesMapper
}

trait SparkRelationshipsMapper extends SparkEntitySource {
  override type Self = SparkRelationshipsMapper

  def withRelationshipTypeName(relTypeName: String): SparkRelationshipsMapper
  def withRelationshipTypeColumn(relTypeColumn: Int): SparkRelationshipsMapper
}





