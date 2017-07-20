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
package org.opencypher.spark.api.spark

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.graph.CypherGraph
import org.opencypher.spark.api.record.{OpaqueField, RecordHeader}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTNode, CTRelationship}

trait SparkCypherGraph extends CypherGraph {

  self =>

  override type Space = SparkGraphSpace
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords
}

object SparkCypherGraph {

  def empty(graphSpace: SparkGraphSpace): SparkCypherGraph =
    EmptyGraph(graphSpace)

  private sealed case class EmptyGraph(
    graphSpace: SparkGraphSpace
  ) extends SparkCypherGraph {

    override def nodes(name: String): SparkCypherRecords =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(CTNode))))(graphSpace)

    override def relationships(name: String): SparkCypherRecords =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(CTRelationship))))(graphSpace)

    override def space = graphSpace
    override def schema = Schema.empty
  }
}
