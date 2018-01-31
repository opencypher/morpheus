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
package org.opencypher.caps.api.schema

import org.apache.spark.sql.DataFrame
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.Entity.sourceIdKey
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.spark._
import org.opencypher.caps.impl.spark.convert.SparkUtils._
import org.opencypher.caps.impl.util.Annotation

import scala.reflect.runtime.universe._

/**
  * An entity table describes how to map an input data frame to a Cypher entity (i.e. nodes or relationships).
  */
sealed trait EntityTable {

  def schema: Schema

  def mapping: EntityMapping

  def table: DataFrame

  // TODO: create CTEntity type
  private[caps] def entityType: CypherType with DefiniteCypherType = mapping.cypherType

  private[caps] def records(implicit caps: CAPSSession): CAPSRecords = CAPSRecords.create(this)

}

/**
  * A node table describes how to map an input data frame to a Cypher node.
  *
  * @param mapping mapping from input data description to a Cypher node
  * @param table   input data frame
  */
case class NodeTable(mapping: NodeMapping, table: DataFrame)(implicit session: CAPSSession) extends EntityTable {

  override lazy val schema: Schema = {
    // TODO: validate that optional label columns have structfield datatype boolean

    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> cypherTypeForColumn(table, sourceKey)
    }

    mapping.optionalLabelMapping.keys.toSet.subsets
      .map(_.union(mapping.impliedLabels))
      .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
      .reduce(_ ++ _)
  }
}

object NodeTable {

  def apply[E <: Node : TypeTag](nodes: Seq[E])(implicit caps: CAPSSession): NodeTable = {
    val nodeLabels = Annotation.labels[E]
    val nodeDF = caps.sparkSession.createDataFrame(nodes)
    val nodeProperties = properties(nodeDF.columns)
    val nodeMapping = NodeMapping.create(nodeIdKey = sourceIdKey, impliedLabels = nodeLabels, propertyKeys = nodeProperties)
    NodeTable(nodeMapping, nodeDF)
  }

  private def properties(nodeColumnNames: Seq[String]): Set[String] = {
    nodeColumnNames.filter(_ != sourceIdKey).toSet
  }
}

/**
  * A relationship table describes how to map an input data frame to a Cypher relationship.
  *
  * @param mapping mapping from input data description to a Cypher relationship
  * @param table   input data frame
  */
case class RelationshipTable(mapping: RelationshipMapping, table: DataFrame)(implicit session: CAPSSession) extends EntityTable {

  override lazy val schema: Schema = {
    val relTypes = mapping.relTypeOrSourceRelTypeKey match {
      case Left(name) => Set(name)
      case Right((_, possibleTypes)) => possibleTypes
    }

    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> cypherTypeForColumn(table, sourceKey)
    }

    relTypes.foldLeft(Schema.empty) {
      case (partialSchema, relType) => partialSchema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
    }
  }
}

object RelationshipTable {

  def apply[E <: Relationship : TypeTag](relationships: Seq[E])(implicit caps: CAPSSession): RelationshipTable = {
    val relationshipType: String = Annotation.relType[E]
    val relationshipDF = caps.sparkSession.createDataFrame(relationships)
    val relationshipProperties = properties(relationshipDF.columns.toSet)

    val relationshipMapping = RelationshipMapping.create(sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relationshipType,
      relationshipProperties)

    RelationshipTable(relationshipMapping, relationshipDF)
  }

  private def properties(relColumnNames: Set[String]): Set[String] = {
    relColumnNames.filter(!Relationship.nonPropertyAttributes.contains(_))
  }
}
