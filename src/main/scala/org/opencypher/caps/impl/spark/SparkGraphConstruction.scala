/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.impl.spark

import org.apache.spark.sql.DataFrame
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.spark.{CAPSGraph, SparkGraphSpace}

trait SparkGraphConstruction {

  self: SparkGraphSpace =>

//  def fromSpark: SparkGraphBuilder
}

trait SparkGraphBuilder {
  def withNodesDF(df: DataFrame, id: String): SparkNodesMapper
  def withRelationshipsDF(df: DataFrame, ids: (String, String, String)): SparkRelationshipsMapper
  def graph: CAPSGraph
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
  final def graph: CAPSGraph = and.graph
  def and: SparkGraphBuilder
  def property(name: String, column: String): SparkNodesMapper
  def property(name: String, column: String, cypherType: CypherType): SparkNodesMapper
  def propertiesAsGiven: SparkNodesMapper
}



