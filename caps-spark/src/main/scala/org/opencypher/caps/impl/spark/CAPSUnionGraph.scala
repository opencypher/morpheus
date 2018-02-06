/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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

import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.ir.api.expr.Var

final case class CAPSUnionGraph(graphs: CAPSGraph*)(implicit val session: CAPSSession) extends CAPSGraph {

  private lazy val individualSchemas = graphs.map(_.schema)

  override lazy val schema: Schema = individualSchemas.foldLeft(Schema.empty)(_ ++ _)

  override def cache(): CAPSUnionGraph = map(_.cache())

  override def persist(): CAPSUnionGraph = map(_.persist())

  override def persist(storageLevel: StorageLevel): CAPSUnionGraph = map(_.persist(storageLevel))

  override def unpersist(): CAPSUnionGraph = map(_.unpersist())

  override def unpersist(blocking: Boolean): CAPSUnionGraph = map(_.unpersist(blocking))

  private def map(f: CAPSGraph => CAPSGraph): CAPSUnionGraph =
    CAPSUnionGraph(graphs.map(f): _*)(session)

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val node = Var(name)(nodeCypherType)
    val targetHeader = RecordHeader.nodeFromSchema(node, schema)
    val nodeScans: Seq[CAPSRecords] = graphs
      .filter(nodeCypherType.labels.isEmpty || _.schema.labels.intersect(nodeCypherType.labels).nonEmpty)
      .map(_.nodes(name, nodeCypherType))
    val alignedScans = nodeScans.map(_.alignWith(node, targetHeader))
    // TODO: Only distinct on id column
    alignedScans.reduceOption(_ unionAll (targetHeader, _)).map(_.distinct).getOrElse(CAPSRecords.empty(targetHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val rel = Var(name)(relCypherType)
    val targetHeader = RecordHeader.relationshipFromSchema(rel, schema)
    val relScans: Seq[CAPSRecords] = graphs
      .filter(relCypherType.types.isEmpty || _.schema.relationshipTypes.intersect(relCypherType.types).nonEmpty)
      .map(_.relationships(name, relCypherType))
    val alignedScans = relScans.map(_.alignWith(rel, targetHeader))
    // TODO: Only distinct on id column
    alignedScans.reduceOption(_ unionAll (targetHeader, _)).map(_.distinct).getOrElse(CAPSRecords.empty(targetHeader))
  }

  override def union(other: PropertyGraph): CAPSUnionGraph = other match {
    case other: CAPSUnionGraph =>
      CAPSUnionGraph(graphs ++ other.graphs: _*)
    case _ =>
      CAPSUnionGraph(graphs :+ other.asCaps: _*)
  }
}
