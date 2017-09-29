/**
  * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.opencypher.caps.api.spark

import cats.data.NonEmptyVector
import org.apache.spark.sql.Row
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.ir.api.global.Label

class ScanGraph(val scans: Seq[GraphScan], val schema: Schema, override val tokens: CAPSRecordsTokens)
  (implicit val session: CAPSSession) extends CAPSGraph {

  // TODO: Caching?

  // TODO: Normalize (ie partition away/remove all optional label fields, rel type fields)
  // TODO: Drop aliases in node scans or here?

  self: CAPSGraph =>

  override protected def graph = this

  private val nodeEntityScans = NodeEntityScans(scans.collect { case it: NodeScan => it }.toVector)
  private val relEntityScans = RelationshipEntityScans(scans.collect { case it: RelationshipScan => it }.toVector)

  override def nodes(name: String, nodeCypherType: CTNode) = {
    val node = Var(name)(nodeCypherType)
    val selectedScans = nodeEntityScans.scans(nodeCypherType)
    val schema = selectedScans.map(_.schema).reduce(_ ++ _)
    val targetNodeHeader = RecordHeader.nodeFromSchema(node, schema, tokens.registry)

    val scanRecords: Seq[CAPSRecords] = selectedScans.map(_.records)
    val alignedRecords = scanRecords.map(GraphScan.align(_, node, targetNodeHeader))
    alignedRecords.reduceOption(_ unionAll(targetNodeHeader, _)).getOrElse(CAPSRecords.empty(targetNodeHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship) = {
    val rel = Var(name)(relCypherType)
    val selectedScans = relEntityScans.scans(relCypherType)
    val schema = selectedScans.map(_.schema).reduce(_ ++ _)
    val targetRelHeader = RecordHeader.relationshipFromSchema(rel, schema, tokens.registry)

    val scanRecords = selectedScans.map(_.records)
    val alignedRecords = scanRecords.map(GraphScan.align(_, rel, targetRelHeader))
    alignedRecords.reduceOption(_ unionAll(targetRelHeader, _)).getOrElse(CAPSRecords.empty(targetRelHeader))
  }

  override def union(other: CAPSGraph): CAPSGraph = other match {
    case (otherScanGraph: ScanGraph) =>
      val allScans = scans ++ otherScanGraph.scans
      val nodeScan = allScans.collectFirst[NodeScan] { case scan: NodeScan => scan }.getOrElse(Raise.impossible())
      CAPSGraph.create(nodeScan, allScans.filterNot(_ == nodeScan): _*)
    case _ => UnionGraph(this, other)
  }

  private case class NodeEntityScans(override val entityScans: Vector[NodeScan]) extends EntityScans {
    override type EntityType = CTNode
    override type EntityScan = NodeScan
  }

  private case class RelationshipEntityScans(override val entityScans: Vector[RelationshipScan]) extends EntityScans {
    override type EntityType = CTRelationship
    override type EntityScan = RelationshipScan
  }

  private trait EntityScans {
    type EntityType <: CypherType with DefiniteCypherType
    type EntityScan <: GraphScan {type EntityCypherType = EntityType}

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
