package org.opencypher.spark.api.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.spark.{ExternalGraph, SparkGraphBuilderImpl}

object DescribeExternalGraph extends SparkGraphBuilderImpl(ExternalGraph(None, None)) {
  def empty = done
}

trait DescribeExternalGraph {
  def withNodesDF(df: DataFrame, id: Int): DescribeExternalNodes
  def withRelationshipsDF(df: DataFrame, ids: (Int, Int, Int)): DescribeExternalRelationships
  def done: ExternalGraph
}

trait DescribeExternalEntities {

  type Self <: DescribeExternalEntities

  final def done: ExternalGraph = and.done

  final def property(name: String, column: Int): Self =
    property(name, column, CTAny.nullable)

  def property(name: String, column: Int, cypherType: CypherType): Self

  def propertiesAsGiven: DescribeExternalGraph
  def and: DescribeExternalGraph
}

trait DescribeExternalNodes extends DescribeExternalEntities {

  override type Self = DescribeExternalNodes

  // def withCommonLabels(labels: String*): SparkNodesMapper

  def label(name: String, column: Int): DescribeExternalNodes
}

trait DescribeExternalRelationships extends DescribeExternalEntities {
  override type Self = DescribeExternalRelationships

  def withRelationshipTypeName(relTypeName: String): DescribeExternalRelationships
  def withRelationshipTypeColumn(relTypeColumn: Int): DescribeExternalRelationships
}





