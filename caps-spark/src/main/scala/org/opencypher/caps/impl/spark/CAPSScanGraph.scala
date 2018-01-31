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

import cats.data.NonEmptyVector
import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.IllegalArgumentException
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.schema.{EntityTable, NodeTable, RelationshipTable, Schema}
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.ir.api.expr._

class CAPSScanGraph(val scans: Seq[EntityTable], val schema: Schema)(implicit val session: CAPSSession)
  extends CAPSGraph {

  // TODO: Normalize (remove redundant columns for implied Schema information, clear aliases?)

  self: CAPSGraph =>

  private val nodeEntityTables = EntityTables(scans.collect { case it: NodeTable => it }.toVector)
  private val relEntityTables = EntityTables(scans.collect { case it: RelationshipTable => it }.toVector)

  override def cache(): CAPSScanGraph = forEach(_.table.cache())

  override def persist(): CAPSScanGraph = forEach(_.table.persist())

  private def forEach(f: EntityTable => Unit): CAPSScanGraph = {
    scans.foreach(f)
    this
  }

  override def persist(storageLevel: StorageLevel): CAPSScanGraph = forEach(_.table.persist(storageLevel))

  override def unpersist(): CAPSScanGraph = forEach(_.table.unpersist())

  override def unpersist(blocking: Boolean): CAPSScanGraph = forEach(_.table.unpersist(blocking))

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val node = Var(name)(nodeCypherType)
    val selectedTables = nodeEntityTables.byType(nodeCypherType)
    val schema = selectedTables.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetNodeHeader = RecordHeader.nodeFromSchema(node, schema)

    val scanRecords: Seq[CAPSRecords] = selectedTables.map(_.records)
    val alignedRecords = scanRecords.map(_.alignWith(node, targetNodeHeader))
    alignedRecords.reduceOption(_ unionAll(targetNodeHeader, _)).getOrElse(CAPSRecords.empty(targetNodeHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val rel = Var(name)(relCypherType)
    val selectedScans = relEntityTables.byType(relCypherType)
    val schema = selectedScans.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetRelHeader = RecordHeader.relationshipFromSchema(rel, schema)

    val scanRecords = selectedScans.map(_.records)
    val alignedRecords = scanRecords.map(_.alignWith(rel, targetRelHeader))
    alignedRecords.reduceOption(_ unionAll(targetRelHeader, _)).getOrElse(CAPSRecords.empty(targetRelHeader))
  }

  override def union(other: PropertyGraph): CAPSGraph = other match {
    case (otherScanGraph: CAPSScanGraph) =>
      val allScans = scans ++ otherScanGraph.scans
      val nodeTable = allScans
        .collectFirst[NodeTable] { case table: NodeTable => table }
        .getOrElse(throw IllegalArgumentException("at least one node scan"))
      CAPSGraph.create(nodeTable, allScans.filterNot(_ == nodeTable): _*)
    case _ => CAPSUnionGraph(this, other.asCaps)
  }

  override protected def graph: CAPSScanGraph = this

  // TODO: add test case where there are multiple rel types in the underlying DF and see if it filters the right one
  case class EntityTables(entityTables: Vector[EntityTable]) {
    type EntityType = CypherType with DefiniteCypherType

    lazy val entityScanTypes: Vector[EntityType] = entityTables.map(_.entityType)

    lazy val entityScansByType: Map[EntityType, NonEmptyVector[EntityTable]] =
      entityTables
        .groupBy(_.entityType)
        .flatMap { case (k, entityScans) => NonEmptyVector.fromVector(entityScans).map(k -> _) }

    def byType(entityType: EntityType): Seq[EntityTable] = {

      def isSubType(tableType: EntityType) = tableType.subTypeOf(entityType).isTrue

      val candidateTypes = entityScanTypes.filter(isSubType)
      // TODO: we always select the head of the NonEmptyVector, why not changing to Map[EntityType, EntityScan]?
      // TODO: does this work for relationships?
      val selectedScans = candidateTypes.flatMap(typ => entityScansByType.get(typ).map(_.head))
      selectedScans
    }
  }
}
