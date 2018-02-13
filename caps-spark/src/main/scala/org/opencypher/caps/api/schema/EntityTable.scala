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
import org.apache.spark.storage.StorageLevel
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.Entity.sourceIdKey
import org.opencypher.caps.api.schema.EntityTable._
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.api.value.CypherValue.CypherValue
import org.opencypher.caps.impl.table.CypherTable
import org.opencypher.caps.impl.spark.DataFrameOps._
import org.opencypher.caps.impl.spark._
import org.opencypher.caps.impl.util.Annotation

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

trait CAPSEntityTable extends EntityTable[SparkTable] {
  // TODO: create CTEntity type
  private[caps] def entityType: CypherType with DefiniteCypherType = mapping.cypherType

  private[caps] def records(implicit caps: CAPSSession): CAPSRecords = CAPSRecords.create(this)
}

case class CAPSNodeTable(mapping: NodeMapping, table: SparkTable) extends NodeTable(mapping, table) with CAPSEntityTable

object CAPSNodeTable {

  def apply[E <: Node : TypeTag](nodes: Seq[E])(implicit caps: CAPSSession): CAPSNodeTable = {
    val nodeLabels = Annotation.labels[E]
    val nodeDF = caps.sparkSession.createDataFrame(nodes)
    val nodeProperties = properties(nodeDF.columns)
    val nodeMapping = NodeMapping.create(nodeIdKey = sourceIdKey, impliedLabels = nodeLabels, propertyKeys = nodeProperties)
    CAPSNodeTable(nodeMapping, nodeDF)
  }

  private def properties(nodeColumnNames: Seq[String]): Set[String] = {
    nodeColumnNames.filter(_ != sourceIdKey).toSet
  }
}

case class CAPSRelationshipTable(mapping: RelationshipMapping, table: SparkTable) extends RelationshipTable(mapping, table) with CAPSEntityTable

object CAPSRelationshipTable {

  def apply[E <: Relationship : TypeTag](relationships: Seq[E])(implicit caps: CAPSSession): CAPSRelationshipTable = {
    val relationshipType: String = Annotation.relType[E]
    val relationshipDF = caps.sparkSession.createDataFrame(relationships)
    val relationshipProperties = properties(relationshipDF.columns.toSet)

    val relationshipMapping = RelationshipMapping.create(sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relationshipType,
      relationshipProperties)

    CAPSRelationshipTable(relationshipMapping, relationshipDF)
  }

  private def properties(relColumnNames: Set[String]): Set[String] = {
    relColumnNames.filter(!Relationship.nonPropertyAttributes.contains(_))
  }
}

/**
  * An entity table describes how to map an input data frame to a Cypher entity (i.e. nodes or relationships).
  */
sealed trait EntityTable[T <: CypherTable[String]] {

  verify()

  def schema: Schema

  def mapping: EntityMapping

  def table: T

  protected def verify(): Unit = {
    table.verifyColumnType(mapping.sourceIdKey, CTInteger, "id key")
  }

}

object EntityTable {

  implicit class SparkTable(val df: DataFrame) extends CypherTable[String] {

    override def columns: Seq[String] = df.columns

    override def columnType: Map[String, CypherType] = columns.map(c => c -> df.cypherTypeForColumn(c)).toMap

    override def rows: Iterator[String => CypherValue] = df.toLocalIterator.asScala.map { row =>
      columns.map(c => c -> CypherValue(row.get(row.fieldIndex(c)))).toMap
    }

    override def size: Long = df.size

    def cache(): SparkTable = df.cache()

    def persist(): SparkTable = df.persist()

    def persist(newLevel: StorageLevel): SparkTable = df.persist(newLevel)

    def unpersist(): SparkTable = df.unpersist()

    def unpersist(blocking: Boolean): SparkTable = df.unpersist(blocking)

  }

}

/**
  * A node table describes how to map an input data frame to a Cypher node.
  *
  * @param mapping mapping from input data description to a Cypher node
  * @param table   input data frame
  */
abstract class NodeTable[T <: CypherTable[String]](mapping: NodeMapping, table: T) extends EntityTable[T] {

  override lazy val schema: Schema = {
    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
    }

    mapping.optionalLabelMapping.keys.toSet.subsets
      .map(_.union(mapping.impliedLabels))
      .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
      .reduce(_ ++ _)
  }

  override protected def verify(): Unit = {
    super.verify()
    mapping.optionalLabelMapping.values.foreach { optionalLabelKey =>
      table.verifyColumnType(optionalLabelKey, CTBoolean, "optional label")
    }
  }
}

/**
  * A relationship table describes how to map an input data frame to a Cypher relationship.
  *
  * @param mapping mapping from input data description to a Cypher relationship
  * @param table   input data frame
  */
abstract class RelationshipTable[T <: CypherTable[String]](mapping: RelationshipMapping, table: T) extends EntityTable[T] {

  override lazy val schema: Schema = {
    val relTypes = mapping.relTypeOrSourceRelTypeKey match {
      case Left(name) => Set(name)
      case Right((_, possibleTypes)) => possibleTypes
    }

    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
    }

    relTypes.foldLeft(Schema.empty) {
      case (partialSchema, relType) => partialSchema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
    }
  }

  override protected def verify(): Unit = {
    super.verify()
    table.verifyColumnType(mapping.sourceStartNodeKey, CTInteger, "start node")
    table.verifyColumnType(mapping.sourceEndNodeKey, CTInteger, "end node")
    mapping.relTypeOrSourceRelTypeKey.right.foreach { key =>
      table.verifyColumnType(key._1, CTString, "relationship type")
    }
  }
}
