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
    mapping.idKeys.values.toSeq.flatten.foreach(key => table.verifyColumnType(key._2, CTIdentity, "id key"))
    if (table.physicalColumns.toSet != mapping.allSourceKeys.toSet) throw IllegalArgumentException(
      s"Columns: ${mapping.allSourceKeys.mkString(", ")}",
      s"Columns: ${table.physicalColumns.mkString(", ")}",
      s"Use CAPS[Node|Relationship]Table#fromMapping to create a valid EntityTable")
  }
}

/**
  * A node table describes how to map an input data frame to a Cypher node.
  *
  * A node table needs to have the canonical column ordering specified by [[org.opencypher.okapi.api.io.conversion.EntityMapping#allSourceKeys]].
  * The easiest way to transform the table to a canonical column ordering is to use one of the constructors on the
  * companion object.
  *
  * Column names prefixed with `property#` are decoded by [[org.opencypher.okapi.impl.util.StringEncodingUtilities]] to
  * recover the original property name.
  *
  * @param mapping mapping from input data description to a Cypher node
  * @param table   input data frame
  */
//abstract class NodeTable[T <: Table[T]](mapping: NodeMapping, table: T)
//  extends EntityTable[T] {
//
//  override lazy val schema: Schema = {
//    val propertyKeys = mapping.propertyMapping.toSeq.map {
//      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
//    }
//
//    mapping.optionalNodeLabelMapping.keys.toSet.subsets
//      .map(_.union(mapping.impliedNodeLabels))
//      .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
//      .reduce(_ ++ _)
//  }
//
//  override protected def verify(): Unit = {
//    super.verify()
//    mapping.optionalNodeLabelMapping.values.foreach { optionalLabelKey =>
//      table.verifyColumnType(optionalLabelKey, CTBoolean, "optional label")
//    }
//  }
//}

/**
  * A relationship table describes how to map an input data frame to a Cypher relationship.
  *
  * A relationship table needs to have the canonical column ordering specified by [[org.opencypher.okapi.api.io.conversion.EntityMapping#allSourceKeys]].
  * The easiest way to transform the table to a canonical column ordering is to use one of the constructors on the
  * companion object.
  *
  * @param mapping mapping from input data description to a Cypher relationship
  * @param table   input data frame
  */
//abstract class RelationshipTable[T <: Table[T]](mapping: RelationshipMapping, table: T)
//  extends EntityTable[T] {
//
//  override lazy val schema: Schema = {
//    val relTypes = mapping.relTypeOrSourceRelTypeKey match {
//      case Left(name) => Set(name)
//      case Right((_, possibleTypes)) => possibleTypes
//    }
//
//    val propertyKeys = mapping.relationshipPropertyMapping.toSeq.map {
//      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
//    }
//
//    relTypes.foldLeft(Schema.empty) {
//      case (partialSchema, relType) => partialSchema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
//    }
//  }
//
//  override protected def verify(): Unit = {
//    super.verify()
//    table.verifyColumnType(mapping.relationshipStartNodeKey, CTInteger, "start node")
//    table.verifyColumnType(mapping.relationshipEndNodeKey, CTInteger, "end node")
//    mapping.relTypeOrSourceRelTypeKey.right.map { case (_, relTypes) =>
//      relTypes.foreach { relType =>
//        table.verifyColumnType(relType.toRelTypeColumnName, CTBoolean, "relationship type")
//      }
//    }
//  }
//}

//object RelationalEntityMapping {
//
//  implicit class EntityMappingOps(val mapping: EntityMapping) {
//
//    def id(v: Var): (Var, String) = v -> mapping.sourceIdKey
//
//    def properties(
//      v: Var,
//      columnToCypherType: Map[String, CypherType]
//    ): Map[Property, String] = mapping.propertyMapping.map {
//      case (key, sourceColumn) => Property(v, PropertyKey(key))(columnToCypherType(sourceColumn)) -> sourceColumn
//    }
//  }
//
//  implicit class NodeMappingOps(val mapping: NodeMapping) {
//
//    def optionalLabels(node: Var): Map[HasLabel, String] = mapping.optionalNodeLabelMapping.map {
//      case (label, sourceColumn) => HasLabel(node, Label(label))(CTBoolean) -> sourceColumn
//    }
//  }
//
//  implicit class RelationshipMappingOps(val mapping: RelationshipMapping) {
//
//    def relTypes(rel: Var): Map[HasType, String] = mapping.relTypeOrSourceRelTypeKey match {
//      case Right((_, names)) =>
//        names.map(name => HasType(rel, RelType(name))(CTBoolean) -> name.toRelTypeColumnName).toMap
//      case Left(_) =>
//        Map.empty
//    }
//
//    def startNode(rel: Var): (StartNode, String) = StartNode(rel)(CTNode) -> mapping.relationshipStartNodeKey
//
//    def endNode(rel: Var): (EndNode, String) = EndNode(rel)(CTNode) -> mapping.relationshipEndNodeKey
//  }
//
//}
