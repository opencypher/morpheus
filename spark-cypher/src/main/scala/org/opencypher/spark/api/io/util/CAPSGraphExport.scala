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
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.ir.api.expr.{Property, Var}
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.spark.api.io.{GraphEntity, Relationship}
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable

// TODO: Add documentation that describes the canonical table format
object CAPSGraphExport {

  implicit class CanonicalTableSparkSchema(val schema: Schema) extends AnyVal {

    def canonicalNodeStructType(labels: Set[String]): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, LongType, nullable = false)
      val properties = schema.nodeKeys(labels).toSeq
        .map { case (propertyName, cypherType) => propertyName.toPropertyColumnName -> cypherType }
        .sortBy { case (propertyColumnName, _) => propertyColumnName }
        .map { case (propertyColumnName, cypherType) =>
          StructField(propertyColumnName, cypherType.getSparkType, cypherType.isNullable)
        }
      StructType(id +: properties)
    }

    def canonicalRelStructType(relType: String): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, LongType, nullable = false)
      val sourceId = StructField(Relationship.sourceStartNodeKey, LongType, nullable = false)
      val targetId = StructField(Relationship.sourceEndNodeKey, LongType, nullable = false)
      val properties = schema.relationshipKeys(relType).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        StructField(propertyName.toPropertyColumnName, cypherType.getSparkType, cypherType.isNullable)
      }
      StructType(id +: sourceId +: targetId +: properties)
    }
  }

  implicit class CanonicalTableExport(graph: RelationalCypherGraph[DataFrameTable]) {

    def canonicalNodeTable(labels: Set[String]): DataFrame = {
      val ct = CTNode(labels)
      val v = Var("n")(ct)
      val nodeRecords = graph.nodes(v.name, ct, exactLabelMatch = true)
      val header = nodeRecords.header

      val idRenaming = header.column(v) -> GraphEntity.sourceIdKey
      val properties: Set[Property] = header.propertiesFor(v)
      val propertyRenamings = properties.map { p => header.column(p) -> p.key.name.toPropertyColumnName }

      val selectColumns = (idRenaming :: propertyRenamings.toList.sortBy(_._2)).map {
        case (oldName, newName) => nodeRecords.table.df.col(oldName).as(newName)
      }

      nodeRecords.table.df.select(selectColumns: _*)
    }

    def canonicalRelationshipTable(relType: String): DataFrame = {
      val ct = CTRelationship(relType)
      val v = Var("r")(ct)
      val relRecords = graph.relationships(v.name, ct)
      val header = relRecords.header

      val idRenaming = header.column(v) -> GraphEntity.sourceIdKey
      val sourceIdRenaming = header.column(header.startNodeFor(v)) -> Relationship.sourceStartNodeKey
      val targetIdRenaming = header.column(header.endNodeFor(v)) -> Relationship.sourceEndNodeKey
      val properties: Set[Property] = relRecords.header.propertiesFor(v)
      val propertyRenamings = properties.map { p => relRecords.header.column(p) -> p.key.name.toPropertyColumnName }

      val selectColumns = (idRenaming :: sourceIdRenaming :: targetIdRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => relRecords.table.df.col(oldName).as(newName)
      }

      relRecords.table.df.select(selectColumns: _*)
    }

  }

}
