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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl

import cats.data.NonEmptyVector
import org.apache.spark.sql.functions
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.io.conversion.RelationshipMapping
import org.opencypher.okapi.api.schema._
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType, DefiniteCypherType}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSEntityTable, CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.schema.CAPSSchema

class CAPSScanGraph(val scans: Seq[CAPSEntityTable], val schema: CAPSSchema, val tags: Set[Int])
  (implicit val session: CAPSSession)
  extends CAPSGraph {

  // TODO: Normalize (remove redundant columns for implied Schema information, clear aliases?)

  self: CAPSGraph =>

  override def toString = s"CAPSScanGraph(${scans.map(_.entityType).mkString(", ")})"

  private val nodeEntityTables = EntityTables(scans.collect { case it: CAPSNodeTable => it })

  private val relEntityTables = EntityTables(scans.collect { case it: CAPSRelationshipTable => it })

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

  private class EntityTables(entityTables: Vector[CAPSEntityTable]) {

    type EntityType = CypherType with DefiniteCypherType

    lazy val entityTableTypes: Set[EntityType] = entityTables.map(_.entityType).toSet

    lazy val entityTablesByType: Map[EntityType, NonEmptyVector[CAPSEntityTable]] =
      entityTables
        .groupBy(_.entityType)
        .flatMap { case (k, entityScans) => NonEmptyVector.fromVector(entityScans).map(k -> _) }

    def byExactType(entityType: EntityType): Seq[CAPSEntityTable] = entityTablesByType(entityType).toVector

    def byType(entityType: EntityType): Seq[CAPSEntityTable] = {

      def isSubType(tableType: EntityType) = tableType.subTypeOf(entityType).isTrue

      entityTableTypes
        .filter(isSubType)
        .flatMap(typ => entityTablesByType(typ).toVector).toSeq
    }
  }

  private object EntityTables {

    /**
      * Splits up relation tables containing multiple relationship types into single relationship tables
      */
    def apply(entityTables: Seq[CAPSEntityTable]): EntityTables = new EntityTables(entityTables.flatMap {
      case CAPSRelationshipTable(relMapping@RelationshipMapping(_, _, _, Right((typeColumnName, relTypes)), _), sparkTable) =>
        val typeColumn = sparkTable.df.col(typeColumnName)
        relTypes.map {
          relType =>
            val filteredDf = sparkTable.df
              .filter(typeColumn === functions.lit(relType))
              .drop(typeColumnName)
            CAPSRelationshipTable(relMapping.copy(relTypeOrSourceRelTypeKey = Left(relType)), filteredDf)
        }
      case other => Seq(other)
    }.toVector)
  }

}
