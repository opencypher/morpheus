package org.opencypher.spark.api.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.spark.{DataFrameGraph, DataFrameGraphBuilderImpl}

object DataFrameGraphBuilder extends DataFrameGraphBuilderImpl(DataFrameGraph(None, None))

  trait DataFrameGraphBuilder {
  def withNodesDF(df: DataFrame, id: Int): NodeDataFrameBuilder
  def withRelationshipsDF(df: DataFrame, ids: (Int, Int, Int)): RelationshipDataFrameBuilder
  def build: DataFrameGraph
}

trait EntityDataFrameBuilder {

  type Self <: EntityDataFrameBuilder

  final def build: DataFrameGraph = and.build

  final def property(name: String, column: Int): Self =
    property(name, column, CTAny.nullable)

  def property(name: String, column: Int, cypherType: CypherType): Self

  def propertiesAsGiven: DataFrameGraphBuilder
  def and: DataFrameGraphBuilder
}

trait NodeDataFrameBuilder extends EntityDataFrameBuilder {

  override type Self = NodeDataFrameBuilder

  // TODO: def commonLabels(labels: String*): NodeDataFrameBuilder

  def label(name: String, column: Int): NodeDataFrameBuilder
}

trait RelationshipDataFrameBuilder extends EntityDataFrameBuilder {
  override type Self = RelationshipDataFrameBuilder

  def relationshipTypeName(relTypeName: String): RelationshipDataFrameBuilder
  def relationshipTypeColumn(relTypeColumn: Int): RelationshipDataFrameBuilder
}





