/*
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

import cats.data.NonEmptyVector
import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.caps.impl.spark.exception.Raise

class CAPSScanGraph(val scans: Seq[GraphScan], val schema: Schema)(
    implicit val session: CAPSSession)
    extends CAPSGraph {

  // TODO: Normalize (remove redundant columns for implied Schema information, clear aliases?)

  self: CAPSGraph =>

  override protected def graph: CAPSScanGraph = this

  private val nodeEntityScans = NodeEntityScans(scans.collect { case it: NodeScan => it }.toVector)
  private val relEntityScans = RelationshipEntityScans(scans.collect {
    case it: RelationshipScan => it
  }.toVector)

  override def cache(): CAPSScanGraph = map(_.cache())

  override def persist(): CAPSScanGraph = map(_.persist())

  override def persist(storageLevel: StorageLevel): CAPSScanGraph = map(_.persist(storageLevel))

  override def unpersist(): CAPSScanGraph = map(_.unpersist())

  override def unpersist(blocking: Boolean): CAPSScanGraph = map(_.unpersist(blocking))

  private def map(f: GraphScan => GraphScan): CAPSScanGraph =
    new CAPSScanGraph(scans.map(f), schema)

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = {
    val node             = Var(name)(nodeCypherType)
    val selectedScans    = nodeEntityScans.scans(nodeCypherType)
    val schema           = selectedScans.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetNodeHeader = RecordHeader.nodeFromSchema(node, schema)

    val scanRecords: Seq[CAPSRecords] = selectedScans.map(_.records)
    val alignedRecords                = scanRecords.map(GraphScan.align(_, node, targetNodeHeader))
    alignedRecords
      .reduceOption(_ unionAll (targetNodeHeader, _))
      .getOrElse(CAPSRecords.empty(targetNodeHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val rel             = Var(name)(relCypherType)
    val selectedScans   = relEntityScans.scans(relCypherType)
    val schema          = selectedScans.map(_.schema).foldLeft(Schema.empty)(_ ++ _)
    val targetRelHeader = RecordHeader.relationshipFromSchema(rel, schema)

    val scanRecords    = selectedScans.map(_.records)
    val alignedRecords = scanRecords.map(GraphScan.align(_, rel, targetRelHeader))
    alignedRecords
      .reduceOption(_ unionAll (targetRelHeader, _))
      .getOrElse(CAPSRecords.empty(targetRelHeader))
  }

  override def union(other: CAPSGraph): CAPSGraph = other match {
    case (otherScanGraph: CAPSScanGraph) =>
      val allScans = scans ++ otherScanGraph.scans
      val nodeScan = allScans
        .collectFirst[NodeScan] { case scan: NodeScan => scan }
        .getOrElse(Raise.impossible())
      CAPSGraph.create(nodeScan, allScans.filterNot(_ == nodeScan): _*)
    case _ => CAPSUnionGraph(this, other)
  }

  private case class NodeEntityScans(override val entityScans: Vector[NodeScan])
      extends EntityScans {
    override type EntityType = CTNode
    override type EntityScan = NodeScan
  }

  private case class RelationshipEntityScans(override val entityScans: Vector[RelationshipScan])
      extends EntityScans {
    override type EntityType = CTRelationship
    override type EntityScan = RelationshipScan
  }

  private trait EntityScans {
    type EntityType <: CypherType with DefiniteCypherType
    type EntityScan <: GraphScan { type EntityCypherType = EntityType }

    def entityScans: Vector[EntityScan]

    lazy val entityScanTypes: Seq[EntityType] = entityScans.map(_.entityType)

    lazy val entityScansByType: Map[EntityType, NonEmptyVector[EntityScan]] =
      entityScans
        .groupBy(_.entityType)
        .flatMap { case (k, entityScans) => NonEmptyVector.fromVector(entityScans).map(k -> _) }

    def scans(entityType: EntityType): Seq[EntityScan] = {
      val candidateTypes = entityScanTypes.filter(_.subTypeOf(entityType).isTrue)
      // TODO: we always select the head of the NonEmptyVector, why not changing to Map[EntityType, EntityScan]?
      // TODO: does this work for relationships?
      val selectedScans = candidateTypes.flatMap(typ => entityScansByType.get(typ).map(_.head))
      selectedScans
    }
  }

}
