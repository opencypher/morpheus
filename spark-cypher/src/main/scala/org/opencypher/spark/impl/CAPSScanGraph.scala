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
package org.opencypher.spark.impl

import cats.data.NonEmptyVector
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.schema._
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.schema.CAPSSchema

class CAPSScanGraph(val scans: Seq[CAPSEntityTable], val schema: CAPSSchema)(implicit val session: CAPSSession)
  extends CAPSGraph {

  // TODO: Normalize (remove redundant columns for implied Schema information, clear aliases?)

  self: CAPSGraph =>

  private val nodeEntityTables = EntityTables(scans.collect { case it: CAPSNodeTable => it }.toVector)
  private val relEntityTables = EntityTables(scans.collect { case it: CAPSRelationshipTable => it }.toVector)

  override def cache(): CAPSScanGraph = forEach(_.table.cache())

  override def persist(): CAPSScanGraph = forEach(_.table.persist())

  private def forEach(f: CAPSEntityTable => Unit): CAPSScanGraph = {
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
        .collectFirst[CAPSNodeTable] { case table: CAPSNodeTable => table }
        .getOrElse(throw IllegalArgumentException("at least one node scan"))
      CAPSGraph.create(nodeTable, allScans.filterNot(_ == nodeTable): _*)
    case _ => CAPSUnionGraph(this, other.asCaps)
  }

  // TODO: add test case where there are multiple rel types in the underlying DF and see if it filters the right one
  case class EntityTables(entityTables: Vector[CAPSEntityTable]) {
    type EntityType = CypherType with DefiniteCypherType

    lazy val entityTableTypes: Vector[EntityType] = entityTables.map(_.entityType)

    lazy val entityTablesByType: Map[EntityType, NonEmptyVector[CAPSEntityTable]] =
      entityTables
        .groupBy(_.entityType)
        .flatMap { case (k, entityScans) => NonEmptyVector.fromVector(entityScans).map(k -> _) }

    def byType(entityType: EntityType): Seq[CAPSEntityTable] = {

      def isSubType(tableType: EntityType) = tableType.subTypeOf(entityType).isTrue

      val candidateTypes = entityTableTypes.filter(isSubType)
      // TODO: we always select the head of the NonEmptyVector, why not changing to Map[EntityType, EntityScan]?
      // TODO: does this work for relationships?
      val selectedScans = candidateTypes.flatMap(typ => entityTablesByType.get(typ).map(_.head))
      selectedScans
    }
  }
}
