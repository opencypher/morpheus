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
package org.opencypher.caps.api.spark

import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.graph.CypherGraph
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.impl.spark.exception.Raise

trait CAPSGraph extends CypherGraph with Serializable {

  self =>

  final override type Graph = CAPSGraph
  final override type Records = CAPSRecords
  final override type Session = CAPSSession
  final override type Result = CAPSResult

  def cache(): CAPSGraph

  def persist(): CAPSGraph

  def persist(storageLevel: StorageLevel): CAPSGraph

  def unpersist(): CAPSGraph

  def unpersist(blocking: Boolean): CAPSGraph
}

object CAPSGraph {

  def empty(implicit caps: CAPSSession): CAPSGraph =
    new EmptyGraph() {
      override protected def graph: CAPSGraph = this

      override def session: CAPSSession = caps

      override def cache(): CAPSGraph = this
      override def persist(): CAPSGraph = this
      override def persist(storageLevel: StorageLevel): CAPSGraph = this
      override def unpersist(): CAPSGraph = this
      override def unpersist(blocking: Boolean): CAPSGraph = this
    }

  def create(nodes: NodeScan, scans: GraphScan*)(implicit caps: CAPSSession): CAPSGraph = {
    val allScans = nodes +: scans
    val schema = allScans.map(_.schema).reduce(_ ++ _)
    new CAPSScanGraph(allScans, schema)
  }

  def create(records: CAPSRecords, schema: Schema)
    (implicit caps: CAPSSession): CAPSGraph = {

    new CAPSPatternGraph(records, schema)
  }

  def createLazy(theSchema: Schema, loadGraph: => CAPSGraph)(implicit caps: CAPSSession): CAPSGraph =
    new LazyGraph(theSchema, loadGraph) {}

  sealed abstract class LazyGraph(override val schema: Schema, loadGraph: => CAPSGraph)(implicit caps: CAPSSession)
    extends CAPSGraph {
    override protected lazy val graph: CAPSGraph = {
      val g = loadGraph
      if (g.schema == schema) g else Raise.schemaMismatch()
    }

    override def session: CAPSSession = caps

    override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords =
      graph.nodes(name, nodeCypherType)

    override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords =
      graph.relationships(name, relCypherType)

    override def union(other: CAPSGraph): CAPSGraph =
      graph.union(other)

    override def cache(): CAPSGraph = { graph.cache(); this }
    override def persist(): CAPSGraph = { graph.persist(); this }
    override def persist(storageLevel: StorageLevel): CAPSGraph = {
      graph.persist(storageLevel)
      this
    }
    override def unpersist(): CAPSGraph = { graph.unpersist(); this }
    override def unpersist(blocking: Boolean): CAPSGraph = { graph.unpersist(blocking); this }
  }

  sealed abstract class EmptyGraph(implicit val caps: CAPSSession) extends CAPSGraph {

    override val schema: Schema = Schema.empty

    override def nodes(name: String, cypherType: CTNode): CAPSRecords =
      CAPSRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def relationships(name: String, cypherType: CTRelationship): CAPSRecords =
      CAPSRecords.empty(RecordHeader.from(OpaqueField(Var(name)(cypherType))))

    override def union(other: CAPSGraph): CAPSGraph = other
  }
}
