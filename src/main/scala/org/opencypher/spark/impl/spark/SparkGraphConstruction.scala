package org.opencypher.spark.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.types._
import org.opencypher.spark.api.spark.{SparkCypherGraph, SparkGraphSpace}

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



