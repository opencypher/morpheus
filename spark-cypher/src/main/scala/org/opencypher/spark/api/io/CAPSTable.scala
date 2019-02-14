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
package org.opencypher.spark.api.io

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.table.RelationalEntityTableFactory
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.table.SparkTable.{DataFrameTable, _}
import org.opencypher.spark.impl.util.Annotation
import org.opencypher.spark.impl.{CAPSRecords, RecordBehaviour}

import scala.reflect.runtime.universe._

case object CAPSEntityTableFactory extends RelationalEntityTableFactory[DataFrameTable] {
  override def entityTable(
    nodeMapping: EntityMapping,
    table: DataFrameTable
  ): EntityTable[DataFrameTable] = {
    CAPSEntityTable.create(nodeMapping, table)
  }
}

case class CAPSEntityTable private[spark](
  override val mapping: EntityMapping,
  override val table: DataFrameTable
) extends EntityTable[DataFrameTable] with RecordBehaviour {

  override type Records = CAPSEntityTable

  private[spark] def records(implicit caps: CAPSSession): CAPSRecords = caps.records.fromEntityTable(entityTable = this)

  override def cache(): CAPSEntityTable = {
    table.cache()
    this
  }
}

object CAPSEntityTable {
  def create(mapping: EntityMapping, table: DataFrameTable): CAPSEntityTable = {
    val colsToSelect = mapping.allSourceKeys
    CAPSEntityTable(mapping, table.select(colsToSelect: _*))
  }
}

object CAPSNodeTable {

  def apply[E <: Node : TypeTag](nodes: Seq[E])(implicit caps: CAPSSession): CAPSEntityTable = {
    val nodeLabels = Annotation.labels[E]
    val nodeDF = caps.sparkSession.createDataFrame(nodes)
    val nodeProperties = nodeDF.columns.filter(_ != GraphEntity.sourceIdKey).toSet
    val nodeMapping = NodeMapping.create(nodeIdKey = GraphEntity.sourceIdKey, impliedLabels = nodeLabels, propertyKeys = nodeProperties)
    CAPSEntityTable.create(nodeMapping, nodeDF)
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
  def apply(impliedLabels: Set[String], nodeDF: DataFrame): CAPSEntityTable =
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
  def apply(impliedLabels: Set[String], optionalLabels: Map[String, String], nodeDF: DataFrame): CAPSEntityTable = {
    val propertyColumnNames = nodeDF.columns.filter(_ != GraphEntity.sourceIdKey).toSet -- optionalLabels.values
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = NodeMapping
      .on(GraphEntity.sourceIdKey)
      .withImpliedLabels(impliedLabels.toSeq: _*)
      .withOptionalLabelMappings(optionalLabels.toSeq: _*)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    CAPSEntityTable.create(mapping, nodeDF)
  }
}

object CAPSRelationshipTable {

  def apply[E <: Relationship : TypeTag](relationships: Seq[E])(implicit caps: CAPSSession): CAPSEntityTable = {
    val relationshipType: String = Annotation.relType[E]
    val relationshipDF = caps.sparkSession.createDataFrame(relationships)
    val relationshipProperties = relationshipDF.columns.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet

    val relationshipMapping = RelationshipMapping.create(GraphEntity.sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relationshipType,
      relationshipProperties)

    CAPSEntityTable.create(relationshipMapping, relationshipDF)
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
  def apply(relationshipType: String, relationshipDF: DataFrame): CAPSEntityTable = {
    val propertyColumnNames = relationshipDF.columns.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = RelationshipMapping
      .on(GraphEntity.sourceIdKey)
      .from(Relationship.sourceStartNodeKey)
      .to(Relationship.sourceEndNodeKey)
      .withRelType(relationshipType)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    CAPSEntityTable.create(mapping, relationshipDF)
  }

  // TODO: Move to unility function
  //  /**
  //    * Creates a relationship table from the given [[RelationshipMapping]] and [[DataFrame]].
  //    *
  //    * @param mapping      relationship mapping
  //    * @param initialTable node data
  //    * @return a relationship table
  //    */
  //  def fromMapping(mapping: RelationshipMapping, initialTable: DataFrame): CAPSEntityTable = {
  //
  //    val updatedTable = mapping.relTypeOrSourceRelTypeKey match {
  //
  //      // Flatten rel type column into boolean columns
  //      case Right((typeColumnName, relTypes)) =>
  //        DataFrameTable(initialTable).verifyColumnType(typeColumnName, CTString, "relationship type")
  //        val updatedTable = relTypes.foldLeft(initialTable) { case (currentDf, relType) =>
  //          val typeColumn = currentDf.col(typeColumnName)
  //          val relTypeColumnName = relType.toRelTypeColumnName
  //          currentDf
  //            .withColumn(relTypeColumnName, typeColumn === functions.lit(relType))
  //        }
  //        updatedTable.drop(updatedTable.col(typeColumnName))
  //
  //      case _ => initialTable
  //    }
  //
  //    val colsToSelect = mapping.allSourceKeys
  //
  //    CAPSEntityTable(mapping, updatedTable.select(colsToSelect.head, colsToSelect.tail: _*))
  //  }

}


