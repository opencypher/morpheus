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
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.{CTNode, CTRelationship}

trait SparkCypherGraph extends CypherGraph with Serializable {

  self =>

  override type Space = SparkGraphSpace
  override type Graph = SparkCypherGraph
  override type Records = SparkCypherRecords
}

object SparkCypherGraph {

  def empty(implicit space: SparkGraphSpace): SparkCypherGraph =
    new EmptyGraph() {}

  def create(nodes: NodeScan, scans: GraphScan*)(implicit space: SparkGraphSpace): SparkCypherGraph = {
    val allScans = nodes +: scans
    val schema = ???
    new ScanGraph(allScans, schema) {}
  }

  sealed abstract class EmptyGraph(implicit val space: SparkGraphSpace) extends SparkCypherGraph {
    override def schema = Schema.empty

    override def nodes(name: String, cypherType: CTNode) =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def relationships(name: String, cypherType: CTRelationship) =
      SparkCypherRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))
  }

  sealed abstract class ScanGraph(val scans: Seq[GraphScan], val schema: Schema)
                                 (implicit val space: SparkGraphSpace) extends SparkCypherGraph {

    private val nodeScans = scans.collect { case it: NodeScan => it }
    private val relScans = scans.collect { case it: RelationshipScan => it }

    override def nodes(name: String, cypherType: CTNode) =
      ???

    override def relationships(name: String, cypherType: CTRelationship) =
      ???
  }
}
