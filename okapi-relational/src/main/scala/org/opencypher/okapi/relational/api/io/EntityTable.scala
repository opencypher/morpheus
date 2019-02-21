/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.relational.api.io

import org.opencypher.okapi.api.graph.{SourceEndNodeKey, SourceIdKey, SourceStartNodeKey}
import org.opencypher.okapi.api.io.conversion.EntityMapping
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.table.RecordHeader

/**
  * An entity table describes how to map an input data frame to a Cypher entity (i.e. nodes or relationships).
  */
trait EntityTable[T <: Table[T]] extends RelationalCypherRecords[T] {

  verify()

  def schema: Schema = {
    mapping.pattern.entities.map { entity =>
      entity.cypherType match {
        case CTNode(impliedTypes, _) =>
          val propertyKeys = mapping.properties(entity).toSeq.map {
            case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
          }

          Schema.empty.withNodePropertyKeys(impliedTypes.toSeq: _*)(propertyKeys: _*)

        case CTRelationship(relTypes, _) =>

          val propertyKeys = mapping.properties(entity).toSeq.map {
            case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
          }

          relTypes.foldLeft(Schema.empty) {
            case (partialSchema, relType) => partialSchema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
          }

        case other => throw IllegalArgumentException("an entity with type CTNode or CTRelationship", other)
      }
    }.reduce(_ ++ _)
  }

  def mapping: EntityMapping

  def header: RecordHeader = {
    mapping.pattern.entities.map { entity =>
      entity.cypherType match {
        case n :CTNode =>
          val nodeVar = Var(entity.name)(n)

          val idMapping = Map(nodeVar -> mapping.idKeys(entity).head._2)

          val propertyMapping = mapping.properties(entity).map {
            case (key, source) => Property(nodeVar, PropertyKey(key))(table.columnType(source)) -> source
          }

          RecordHeader(idMapping ++ propertyMapping)

        case r :CTRelationship =>
          val relVar = Var(entity.name)(r)

          val idMapping = mapping.idKeys(entity).map {
            case (SourceIdKey, source) => relVar -> source
            case (SourceStartNodeKey, source) => StartNode(relVar)(CTNode) -> source
            case (SourceEndNodeKey, source) => EndNode(relVar)(CTNode) -> source
          }

          val propertyMapping = mapping.properties(entity).map {
            case (key, source) => Property(relVar, PropertyKey(key))(table.columnType(source)) -> source
          }

          RecordHeader(idMapping ++ propertyMapping)

        case other => throw IllegalArgumentException("an entity with type CTNode or CTRelationship", other)
      }
    }.reduce(_ ++ _)
  }

  protected def verify(): Unit = {
    mapping.idKeys.values.toSeq.flatten.foreach {
      case (_, column) => table.verifyColumnType(column, CTIdentity, "id key")
    }

    if (table.physicalColumns.toSet != mapping.allSourceKeys.toSet) throw IllegalArgumentException(
      s"Columns: ${mapping.allSourceKeys.mkString(", ")}",
      s"Columns: ${table.physicalColumns.mkString(", ")}",
      s"Use CAPS[Node|Relationship]Table#fromMapping to create a valid EntityTable")
  }
}

