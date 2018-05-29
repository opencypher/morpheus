/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.api.io.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.spark.api.io.util.StringEncodingUtilities._
import org.opencypher.spark.api.io.{GraphEntity, Relationship}
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.convert.CAPSCypherType._

object CAPSGraphExport {

  implicit class CanonicalTableSparkSchema(val schema: Schema) extends AnyVal {

    def canonicalNodeTableSchema(labels: Set[String]): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, LongType, nullable = false)
      val properties = schema.nodeKeys(labels).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        StructField(propertyName.toPropertyColumnName, cypherType.getSparkType, cypherType.isNullable)
      }
      StructType(id +: properties)
    }

    def canonicalRelTableSchema(relType: String): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, LongType, nullable = false)
      val sourceId = StructField(Relationship.sourceStartNodeKey, LongType, nullable = false)
      val targetId = StructField(Relationship.sourceEndNodeKey, LongType, nullable = false)
      val properties = schema.relationshipKeys(relType).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        StructField(propertyName.toPropertyColumnName, cypherType.getSparkType, cypherType.isNullable)
      }
      StructType(id +: sourceId +: targetId +: properties)
    }
  }

  implicit class CanonicalTableExport(graph: CAPSGraph) {

    def canonicalNodeTable(labels: Set[String]): DataFrame = {
      val varName = "n"
      val nodeRecords = graph.nodesWithExactLabels(varName, labels)

      val idRenaming = varName -> GraphEntity.sourceIdKey
      val propertyRenamings = nodeRecords.header.propertySlots(Var(varName)())
        .map { case (p, slot) => nodeRecords.header.of(slot) -> p.key.name.toPropertyColumnName }

      val selectColumns = (idRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => nodeRecords.data.col(oldName).as(newName)
      }

      nodeRecords.data.select(selectColumns: _*)
    }

    def canonicalRelationshipTable(relType: String): DataFrame = {
      val varName = "r"
      val relCypherType = CTRelationship(relType)
      val v = Var(varName)(relCypherType)

      val relRecords = graph.relationships(varName, relCypherType)
      val header = relRecords.header

      val idRenaming = varName -> GraphEntity.sourceIdKey
      val sourceIdRenaming = header.of(header.sourceNodeSlot(v)) -> Relationship.sourceStartNodeKey
      val targetIdRenaming = header.of(header.targetNodeSlot(v)) -> Relationship.sourceEndNodeKey
      val propertyRenamings = header.propertySlots(Var(varName)())
        .map { case (p, slot) => header.of(slot) -> p.key.name.toPropertyColumnName }

      val selectColumns = (idRenaming :: sourceIdRenaming :: targetIdRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => relRecords.data.col(oldName).as(newName)
      }

      relRecords.data.select(selectColumns: _*)
    }

  }

}
