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
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, Table}
import org.opencypher.okapi.relational.impl.table.RecordHeader

/**
  * An entity table describes how to map an input data frame to a Cypher entity (i.e. nodes or relationships).
  */
trait EntityTable[T <: Table[T]] extends RelationalCypherRecords[T] {

  verify()

  def schema: Schema = {
    mapping.pattern.entities.map { entity =>
      entity.typ match {
        case _: CTNode =>
          val propertyKeys = mapping.properties(entity).toSeq.map {
            case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
          }

          mapping.optionalTypes(entity).keys.toSet.subsets
            .map(_.union(mapping.impliedTypes(entity)))
            .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
            .reduce(_ ++ _)

        case _: CTRelationship =>
          val relTypes = mapping.impliedTypes(entity).union(mapping.optionalTypes(entity).keySet)

          val propertyKeys = mapping.properties(entity).toSeq.map {
            case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
          }

          relTypes.foldLeft(Schema.empty) {
            case (partialSchema, relType) => partialSchema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
          }

        case other => ???
      }
    }.reduce(_ ++ _)
  }

  def mapping: EntityMapping

  def header: RecordHeader = {
    mapping.pattern.entities.map { entity =>
      entity.typ match {
        case n :CTNode =>
          val nodeVar = Var(entity.name)(n)

          val idMapping = Map(nodeVar -> mapping.idKeys(entity).head._2)

          val labelMapping = mapping.optionalTypes(entity).map {
            case (label, source) => HasLabel(nodeVar, Label(label))(CTBoolean) -> source
          }

          val propertyMapping = mapping.properties(entity).map {
            case (key, source) => Property(nodeVar, PropertyKey(key))(table.columnType(source)) -> source
          }

          RecordHeader(idMapping ++ labelMapping ++ propertyMapping)

        case r :CTRelationship =>
          val relVar = Var(entity.name)(r)

          val idMapping = mapping.idKeys(entity).map {
            case (SourceIdKey, source) => relVar -> source
            case (SourceStartNodeKey, source) => StartNode(relVar)(CTNode) -> source
            case (SourceEndNodeKey, source) => EndNode(relVar)(CTNode) -> source
          }

          val labelMapping = mapping.optionalTypes(entity).map {
            case (typ, source) => HasType(relVar, RelType(typ))(CTBoolean) -> source
          }

          val propertyMapping = mapping.properties(entity).map {
            case (key, source) => Property(relVar, PropertyKey(key))(table.columnType(source)) -> source
          }

          RecordHeader(idMapping ++ labelMapping ++ propertyMapping)

        case other => ???
      }
    }.reduce(_ ++ _)
  }

  protected def verify(): Unit = {
    mapping.idKeys.values.toSeq.flatten.foreach {
      case (_, column) => table.verifyColumnType(column, CTIdentity, "id key")
    }

    mapping.optionalTypes.values.toSeq.flatten.foreach {
      case (_, column) => table.verifyColumnType(column, CTBoolean, "optional type")
    }

    if (table.physicalColumns.toSet != mapping.allSourceKeys.toSet) throw IllegalArgumentException(
      s"Columns: ${mapping.allSourceKeys.mkString(", ")}",
      s"Columns: ${table.physicalColumns.mkString(", ")}",
      s"Use CAPS[Node|Relationship]Table#fromMapping to create a valid EntityTable")
  }
}

