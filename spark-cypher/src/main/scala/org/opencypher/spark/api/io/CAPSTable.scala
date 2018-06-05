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

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.storage.StorageLevel
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.CypherTable
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.relational.api.io.{EntityTable, FlatRelationalTable}
import org.opencypher.okapi.relational.impl.physical._
import org.opencypher.okapi.relational.impl.util.StringEncodingUtilities
import org.opencypher.okapi.relational.impl.util.StringEncodingUtilities._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.SparkCypherTable.SparkCypherTable
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.util.Annotation
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object SparkCypherTable {

  implicit class SparkCypherTable(val df: DataFrame) extends FlatRelationalTable[SparkCypherTable] {

    override def columns: Seq[String] = df.columns

    override def columnType: Map[String, CypherType] = columns.map(c => c -> df.cypherTypeForColumn(c)).toMap

    override def rows: Iterator[String => CypherValue] = df.toLocalIterator.asScala.map { row =>
      columns.map(c => c -> CypherValue(row.get(row.fieldIndex(c)))).toMap
    }

    override def size: Long = df.count()

    def cache(): SparkCypherTable = df.cache()

    def persist(): SparkCypherTable = df.persist()

    def persist(newLevel: StorageLevel): SparkCypherTable = df.persist(newLevel)

    def unpersist(): SparkCypherTable = df.unpersist()

    def unpersist(blocking: Boolean): SparkCypherTable = df.unpersist(blocking)

    override def select(cols: String*): SparkCypherTable = {
      df.select(cols.head, cols.tail: _*)
    }

    override def unionAll(other: SparkCypherTable): SparkCypherTable = {
      df.union(other.df)
    }

    override def join(other: SparkCypherTable, joinType: JoinType, joinCols: (String, String)*): SparkCypherTable = {
      val joinTypeString = joinType match {
        case InnerJoin => "inner"
        case LeftOuterJoin => "left_outer"
        case RightOuterJoin => "right_outer"
        case FullOuterJoin => "full_outer"
      }

      val joinExpr = joinCols.map {
        case (l, r) => df.col(l) === other.df.col(r)
      }.reduce((acc, expr) => acc && expr)

      df.join(other.df, joinExpr, joinTypeString)
    }

    override def distinct: SparkCypherTable = df.distinct

    override def distinct(cols: String*): SparkCypherTable = df.dropDuplicates(cols)

    override def withNullColumn(col: String): SparkCypherTable = df.withColumn(col, functions.lit(null))

    override def withTrueColumn(col: String): SparkCypherTable = df.withColumn(col, functions.lit(true))

    override def withFalseColumn(col: String): SparkCypherTable = df.withColumn(col, functions.lit(false))
  }

}

trait CAPSEntityTable extends EntityTable[SparkCypherTable] {
  // TODO: create CTEntity type
  private[spark] def entityType: CypherType with DefiniteCypherType = mapping.cypherType

  private[spark] def records(implicit caps: CAPSSession): CAPSRecords = CAPSRecords.create(this)
}

case class CAPSNodeTable(
  mapping: NodeMapping,
  table: SparkCypherTable
) extends NodeTable(mapping, table) with CAPSEntityTable

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
  mapping: RelationshipMapping,
  table: SparkCypherTable
) extends RelationshipTable(mapping, table) with CAPSEntityTable

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
    * Column names prefixed with `property#` are decoded by [[StringEncodingUtilities]] to
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
    val colsToSelect = mapping.allSourceKeys
    CAPSRelationshipTable(mapping, initialTable.select(colsToSelect.head, colsToSelect.tail: _*))
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
  * Column names prefixed with `property#` are decoded by [[StringEncodingUtilities]] to
  * recover the original property name.
  *
  * @param mapping mapping from input data description to a Cypher node
  * @param table   input data frame
  */
abstract class NodeTable[T <: CypherTable](mapping: NodeMapping, table: T) extends EntityTable[T] {

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
abstract class RelationshipTable[T <: CypherTable](
  mapping: RelationshipMapping,
  table: T
) extends EntityTable[T] {

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
    mapping.relTypeOrSourceRelTypeKey.right.foreach { key =>
      table.verifyColumnType(key._1, CTString, "relationship type")
    }
  }
}
