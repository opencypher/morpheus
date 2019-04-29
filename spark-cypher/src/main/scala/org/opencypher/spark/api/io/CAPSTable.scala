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
import org.opencypher.okapi.api.io.conversion.{ElementMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.relational.api.io.ElementTable
import org.opencypher.okapi.relational.api.table.RelationalElementTableFactory
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.table.SparkTable.{DataFrameTable, _}
import org.opencypher.spark.impl.util.Annotation
import org.opencypher.spark.impl.{CAPSRecords, RecordBehaviour}

import scala.reflect.runtime.universe._

case object CAPSElementTableFactory$ extends RelationalElementTableFactory[DataFrameTable] {
  override def elementTable(
    nodeMapping: ElementMapping,
    table: DataFrameTable
  ): ElementTable[DataFrameTable] = {
    CAPSElementTable.create(nodeMapping, table)
  }
}

case class CAPSElementTable private[spark](
  override val mapping: ElementMapping,
  override val table: DataFrameTable
) extends ElementTable[DataFrameTable] with RecordBehaviour {

  override type Records = CAPSElementTable

  private[spark] def records(implicit caps: CAPSSession): CAPSRecords = caps.records.fromElementTable(elementTable = this)

  override def cache(): CAPSElementTable = {
    table.cache()
    this
  }
}

object CAPSElementTable {
  def create(mapping: ElementMapping, table: DataFrameTable): CAPSElementTable = {
    val sourceIdColumns = mapping.allSourceIdKeys
    val idCols = table.df.encodeIdColumns(sourceIdColumns: _*)
    val remainingCols = mapping.allSourcePropertyKeys.map(table.df.col)
    val colsToSelect = idCols ++ remainingCols

    CAPSElementTable(mapping, table.df.select(colsToSelect: _*))
  }
}

object CAPSNodeTable {

  def apply[E <: Node : TypeTag](nodes: Seq[E])(implicit caps: CAPSSession): CAPSElementTable = {
    val nodeLabels = Annotation.labels[E]
    val nodeDF = caps.sparkSession.createDataFrame(nodes)
    val nodeProperties = nodeDF.columns.filter(_ != GraphElement.sourceIdKey).toSet
    val nodeMapping = NodeMappingBuilder.create(nodeIdKey = GraphElement.sourceIdKey, impliedLabels = nodeLabels, propertyKeys = nodeProperties)
    CAPSElementTable.create(nodeMapping, nodeDF)
  }

  /**
    * Creates a node table from the given [[DataFrame]]. By convention, there needs to be one column storing node
    * identifiers and named after [[GraphElement.sourceIdKey]]. All remaining columns are interpreted as node property columns, the column name is used as property
    * key.
    *
    * @param impliedLabels  implied node labels
    * @param nodeDF         node data
    * @return a node table with inferred node mapping
    */
  def apply(impliedLabels: Set[String], nodeDF: DataFrame): CAPSElementTable = {
    val propertyColumnNames = nodeDF.columns.filter(_ != GraphElement.sourceIdKey).toSet
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = NodeMappingBuilder
      .on(GraphElement.sourceIdKey)
      .withImpliedLabels(impliedLabels.toSeq: _*)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    CAPSElementTable.create(mapping, nodeDF)
  }
}

object CAPSRelationshipTable {

  def apply[E <: Relationship : TypeTag](relationships: Seq[E])(implicit caps: CAPSSession): CAPSElementTable = {
    val relationshipType: String = Annotation.relType[E]
    val relationshipDF = caps.sparkSession.createDataFrame(relationships)
    val relationshipProperties = relationshipDF.columns.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet

    val relationshipMapping = RelationshipMappingBuilder.create(GraphElement.sourceIdKey,
      Relationship.sourceStartNodeKey,
      Relationship.sourceEndNodeKey,
      relationshipType,
      relationshipProperties)

    CAPSElementTable.create(relationshipMapping, relationshipDF)
  }

  /**
    * Creates a relationship table from the given [[DataFrame]]. By convention, there needs to be one column storing
    * relationship identifiers and named after [[GraphElement.sourceIdKey]], one column storing source node identifiers
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
  def apply(relationshipType: String, relationshipDF: DataFrame): CAPSElementTable = {
    val propertyColumnNames = relationshipDF.columns.filter(!Relationship.nonPropertyAttributes.contains(_)).toSet
    val propertyKeyMapping = propertyColumnNames.map(p => p.toProperty -> p)

    val mapping = RelationshipMappingBuilder
      .on(GraphElement.sourceIdKey)
      .from(Relationship.sourceStartNodeKey)
      .to(Relationship.sourceEndNodeKey)
      .withRelType(relationshipType)
      .withPropertyKeyMappings(propertyKeyMapping.toSeq: _*)
      .build

    CAPSElementTable.create(mapping, relationshipDF)
  }
}


