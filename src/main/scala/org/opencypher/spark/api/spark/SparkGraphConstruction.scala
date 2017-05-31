package org.opencypher.spark.api.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.types._

trait SparkGraphConstruction {

  self: SparkGraphSpace =>

  def fromSpark: SparkGraphBuilder
}

trait SparkGraphBuilder {
  def withNodesDF(df: DataFrame, id: String): SparkNodesMapper
  def withRelationshipsDF(df: DataFrame, ids: (String, String, String)): SparkRelationshipsMapper
  def graph: SparkCypherGraph
}


trait SparkEntitySource {

  type Self <: SparkEntitySource

  final def graph: SparkCypherGraph = and.graph

  final def property(name: String): Self = property(name, name)
  def property(name: String, column: String): Self

  final def property(name: String, cypherType: CypherType): Self = property(name, name, cypherType)
  def property(name: String, column: String, cypherType: CypherType): Self

  def propertiesAsGiven: SparkGraphBuilder
  def and: SparkGraphBuilder
}

trait SparkNodesMapper extends SparkEntitySource {

  override type Self = SparkNodesMapper

  // def withCommonLabels(labels: String*): SparkNodesMapper

  final def label(name: String): SparkNodesMapper = label(name, name)
  def label(name: String, column: String): SparkNodesMapper
}

trait SparkRelationshipsMapper extends SparkEntitySource {
  override type Self = SparkRelationshipsMapper

  def withRelationshipTypeName(relTypeName: String): SparkRelationshipsMapper
  def withRelationshipTypeColumn(relTypeColumn: String): SparkRelationshipsMapper
}





