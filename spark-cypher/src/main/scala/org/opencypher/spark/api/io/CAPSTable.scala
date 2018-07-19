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
package org.opencypher.spark.api.io

import org.apache.spark.sql.{DataFrame, _}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{DefiniteCypherType, _}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.table.SparkFlatRelationalTable.DataFrameTable
import org.opencypher.spark.impl.util.Annotation
import org.opencypher.spark.impl.{CAPSRecords, RecordBehaviour}
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

import scala.reflect.runtime.universe._

trait CAPSEntityTable extends EntityTable[DataFrameTable] {
  // TODO: create CTEntity type
  private[spark] def entityType: CypherType with DefiniteCypherType

  private[spark] def records(implicit caps: CAPSSession): CAPSRecords = caps.records.fromEntityTable(entityTable = this)
}

case class CAPSNodeTable(
  override val mapping: NodeMapping,
  override val table: DataFrameTable
) extends NodeTable(mapping, table) with CAPSEntityTable {

  override type Records = CAPSNodeTable

  override private[spark] def entityType = mapping.cypherType

  override def cache(): CAPSNodeTable = {
    table.df.cache()
    this
  }
}

object CAPSNodeTable {

  def apply[E <: Node : TypeTag](nodes: Seq[E])(implicit caps: CAPSSession): CAPSNodeTable = {
    val nodeLabels = Annotation.labels[E]
    val nodeDF = caps.sparkSession.createDataFrame(nodes)
    val nodeProperties = properties(nodeDF.columns)
    val nodeMapping = NodeMapping.create(nodeIdKey = GraphEntity.sourceIdKey, impliedLabels = nodeLabels, propertyKeys = nodeProperties)
    fromMapping(nodeMapping, nodeDF)
  }

  /**
    * Creates a node table from the given [[DataFrame]]. By convention, there needs to be one column storing node
    * identifiers and named after [[GraphEntity.sourceIdKey]]. All remaining columns are interpreted as node property
    * columns, the column name is used as property key.
    *
    * @param impliedLabels implied node labels
    * @param nodeDF        node data
    * @return a node table with inferred node mapping
    */
  def apply(impliedLabels: Set[String], nodeDF: DataFrame): CAPSNodeTable =
    CAPSNodeTable(impliedLabels, Map.empty, nodeDF)

  /**
    * Creates a node table from the given [[DataFrame]]. By convention, there needs to be one column storing node
    * identifiers and named after [[GraphEntity.sourceIdKey]]. Optional labels are defined by a mapping from label to
    * column name. All remaining columns are interpreted as node property columns, the column name is used as property
    * key.
    *
    * @param impliedLabels  implied node labels
    * @param optionalLabels mapping from optional labels to column names
    * @param nodeDF         node data
    * @return a node table with inferred node mapping
    */
  def apply(impliedLabels: Set[String], optionalLabels: Map[String, String], nodeDF: DataFrame): CAPSNodeTable = {
    val propertyColumnNames = properties(nodeDF.columns) -- optionalLabels.values

    val baseMapping = NodeMapping(GraphEntity.sourceIdKey, impliedLabels, optionalLabels)

    val nodeMapping = propertyColumnNames.foldLeft(baseMapping) { (mapping, propertyColumn) =>
      mapping.withPropertyKey(propertyColumn.toProperty, propertyColumn)
    }

    fromMapping(nodeMapping, nodeDF)
  }

  /**
    * Creates a node table from the given [[NodeMapping]] and [[DataFrame]].
    *
    * @param mapping      node mapping
    * @param initialTable node data
    * @return a node table
    */
  def fromMapping(mapping: NodeMapping, initialTable: DataFrame): CAPSNodeTable = {
    val colsToSelect = mapping.allSourceKeys
    CAPSNodeTable(mapping, initialTable.select(colsToSelect.head, colsToSelect.tail: _*))
  }

  private def properties(nodeColumnNames: Seq[String]): Set[String] = {
    nodeColumnNames.filter(_ != GraphEntity.sourceIdKey).toSet
  }
}

case class CAPSRelationshipTable(
  override val mapping: RelationshipMapping,
  override val table: DataFrameTable
) extends RelationshipTable(mapping, table) with CAPSEntityTable {

  override type Records = CAPSRelationshipTable

  override private[spark] def entityType = mapping.cypherType

  override def cache(): CAPSRelationshipTable = {
    table.df.cache()
    this
  }
}

object CAPSRelationshipTable {

  def apply[E <: Relationship : TypeTag](relationships: Seq[E])(implicit caps: CAPSSession): CAPSRelationshipTable = {
    val relationshipType: String = Annotation.relType[E]
    val relationshipDF = caps.sparkSession.createDataFrame(relationships)
    val relationshipProperties = properties(relationshipDF.columns)

    val relationshipMapping = RelationshipMapping.create(GraphEntity.sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relationshipType,
      relationshipProperties)

    fromMapping(relationshipMapping, relationshipDF)
  }

  /**
    * Creates a relationship table from the given [[DataFrame]]. By convention, there needs to be one column storing
    * relationship identifiers and named after [[GraphEntity.sourceIdKey]], one column storing source node identifiers
    * and named after [[Relationship.sourceStartNodeKey]] and one column storing target node identifiers and named after
    * [[Relationship.sourceEndNodeKey]]. All remaining columns are interpreted as relationship property columns, the
    * column name is used as property key.
    *
    * Column names prefixed with `property#` are decoded by [[org.opencypher.okapi.impl.util.StringEncodingUtilities]] to
    * recover the original property name.
    *
    * @param relationshipType relationship type
    * @param relationshipDF   relationship data
    * @return a relationship table with inferred relationship mapping
    */
  def apply(relationshipType: String, relationshipDF: DataFrame): CAPSRelationshipTable = {
    val propertyColumnNames = properties(relationshipDF.columns)

    val baseMapping = RelationshipMapping.create(GraphEntity.sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relationshipType)

    val relationshipMapping = propertyColumnNames.foldLeft(baseMapping) { (mapping, propertyColumn) =>
      mapping.withPropertyKey(propertyColumn.toProperty, propertyColumn)
    }

    fromMapping(relationshipMapping, relationshipDF)
  }

  /**
    * Creates a relationship table from the given [[RelationshipMapping]] and [[DataFrame]].
    *
    * @param mapping      relationship mapping
    * @param initialTable node data
    * @return a relationship table
    */
  def fromMapping(mapping: RelationshipMapping, initialTable: DataFrame): CAPSRelationshipTable = {

    val updatedTable = mapping.relTypeOrSourceRelTypeKey match {

      // Flatten rel type column into boolean columns
      case Right((typeColumnName, relTypes)) =>
        DataFrameTable(initialTable).verifyColumnType(typeColumnName, CTString, "relationship type")
        val updatedTable = relTypes.foldLeft(initialTable) { case (currentDf, relType) =>
          val typeColumn = currentDf.col(typeColumnName)
          val relTypeColumnName = relType.toRelTypeColumnName
          currentDf
            .withColumn(relTypeColumnName, typeColumn === functions.lit(relType))
            .setNonNullable(relTypeColumnName)
        }
        updatedTable.drop(updatedTable.col(typeColumnName))

      case _ => initialTable
    }

    val colsToSelect = mapping.allSourceKeys

    CAPSRelationshipTable(mapping, updatedTable.select(colsToSelect.head, colsToSelect.tail: _*))
  }

  private def properties(relColumnNames: Seq[String]): Set[String] = {
    relColumnNames.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet
  }
}

/**
  * A node table describes how to map an input data frame to a Cypher node.
  *
  * A node table needs to have the canonical column ordering specified by [[EntityMapping#allSourceKeys]].
  * The easiest way to transform the table to a canonical column ordering is to use one of the constructors on the
  * companion object.
  *
  * Column names prefixed with `property#` are decoded by [[org.opencypher.okapi.impl.util.StringEncodingUtilities]] to
  * recover the original property name.
  *
  * @param mapping mapping from input data description to a Cypher node
  * @param table   input data frame
  */
abstract class NodeTable(mapping: NodeMapping, table: DataFrameTable)
  extends EntityTable[DataFrameTable] with RecordBehaviour {

  override lazy val schema: CAPSSchema = {
    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
    }

    mapping.optionalLabelMapping.keys.toSet.subsets
      .map(_.union(mapping.impliedLabels))
      .map(combo => Schema.empty.withNodePropertyKeys(combo.toSeq: _*)(propertyKeys: _*))
      .reduce(_ ++ _)
      .asCaps
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
  * A relationship table needs to have the canonical column ordering specified by [[EntityMapping#allSourceKeys]].
  * The easiest way to transform the table to a canonical column ordering is to use one of the constructors on the
  * companion object.
  *
  * @param mapping mapping from input data description to a Cypher relationship
  * @param table   input data frame
  */
abstract class RelationshipTable(mapping: RelationshipMapping, table: DataFrameTable)
  extends EntityTable[DataFrameTable] with RecordBehaviour {

  override lazy val schema: CAPSSchema = {
    val relTypes = mapping.relTypeOrSourceRelTypeKey match {
      case Left(name) => Set(name)
      case Right((_, possibleTypes)) => possibleTypes
    }

    val propertyKeys = mapping.propertyMapping.toSeq.map {
      case (propertyKey, sourceKey) => propertyKey -> table.columnType(sourceKey)
    }

    relTypes.foldLeft(Schema.empty) {
      case (partialSchema, relType) => partialSchema.withRelationshipPropertyKeys(relType)(propertyKeys: _*)
    }.asCaps
  }

  override protected def verify(): Unit = {
    super.verify()
    table.verifyColumnType(mapping.sourceStartNodeKey, CTInteger, "start node")
    table.verifyColumnType(mapping.sourceEndNodeKey, CTInteger, "end node")
    mapping.relTypeOrSourceRelTypeKey.right.map { case (_, relTypes) =>
      relTypes.foreach { relType =>
        table.verifyColumnType(relType.toRelTypeColumnName, CTBoolean, "relationship type")
      }
    }
  }
}
