package org.opencypher.spark.prototype.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkGraphSpace}

trait SparkGraphConstruction {

  self: SparkGraphSpace =>

//  def fromSpark: SparkGraphBuilder
}

trait SparkGraphBuilder {
  def withNodesDF(df: DataFrame, id: String): SparkNodesMapper
  def withRelationshipsDF(df: DataFrame, ids: (String, String, String)): SparkRelationshipsMapper
  def graph: SparkCypherGraph
}

trait SparkNodesMapper {
  def havingLabels(label: String*): SparkEntitySource
}

trait SparkRelationshipsMapper {
  def havingFixedRelationshipType(relType: String): SparkEntitySource
  def havingDynamicRelationshipTypes(relTypes: (String, String)): SparkEntitySource
}
trait SparkNodesSource extends SparkEntitySource {
  def label(label: (String, String)): SparkNodesSource
}

trait SparkEntitySource {
  final def graph: SparkCypherGraph = and.graph
  def and: SparkGraphBuilder
  def property(name: String, column: String): SparkNodesMapper
  def property(name: String, column: String, cypherType: CypherType): SparkNodesMapper
  def propertiesAsGiven: SparkNodesMapper
}



