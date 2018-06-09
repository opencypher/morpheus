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
package org.opencypher.okapi.relational.api.io

import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.table.{CypherRecords, CypherTable}
import org.opencypher.okapi.api.types.{CTBoolean, CTInteger, CTNode, CypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.api.io.RelationalEntityMapping._
import org.opencypher.okapi.relational.impl.physical.{Ascending, Descending, JoinType, Order}
import org.opencypher.okapi.relational.impl.table.RecordHeader

trait FlatRelationalTable[T <: FlatRelationalTable[T]] extends CypherTable {

  this: T =>

  def select(cols: String*): T

  def drop(cols: String*): T

  def unionAll(other: T): T

  def orderBy(sortItems: (String, Order)*): T

  def distinct: T

  def distinct(cols: String*): T

  def withColumnRenamed(oldColumn: String, newColumn: String): T

  def withNullColumn(col: String): T

  def withTrueColumn(col: String): T

  def withFalseColumn(col: String): T

  def join(other: T, joinType: JoinType, joinCols: (String, String)*): T

}

trait RelationalCypherRecords[T <: FlatRelationalTable[T]] extends CypherRecords {

  type R <: RelationalCypherRecords[T]

  def from(header: RecordHeader, table: T): R

  def table: T

  override def columns: Seq[String] = table.columns

  def header: RecordHeader

  def select(exprs: Expr*): R = {
    val selectHeader = header.select(exprs: _*)
    val selectedColumns = selectHeader.columns
    val selectedData = table.select(selectedColumns.toSeq.sorted: _*)
    from(selectHeader, selectedData)
  }

  def select(expr: (Expr, Option[Var]), epxrs: (Expr, Option[Var])*): R = {
    val allExprs = expr +: epxrs
    val aliases = allExprs.collect { case (e, Some(v)) => e -> v }

    val headerWithAliases = header.withAlias(aliases: _*)

    val selectExprs = allExprs.collect {
      case (_, Some(v)) => v
      case (e, None) => e
    }

    val selectHeader = headerWithAliases.select(selectExprs: _*)

    from(selectHeader, table.select(selectExprs.map(headerWithAliases.column).distinct: _*))
  }

  def drop(exprs: Expr*): R = {
    val updatedHeader = header -- exprs.toSet
    val updatedTable = table.drop(exprs.map(header.column): _*)
    from(updatedHeader, updatedTable)
  }

  def withColumnsRenamed(renamings: (Expr, String)*): R = {
    val updatedHeader = renamings.foldLeft(header) {
      case (currentHeader, (expr, newColumn)) => currentHeader.withColumnRenamed(expr, newColumn)
    }
    val updatedTable = renamings.foldLeft(table) {
      case (currentTable, (expr, newColumn)) => currentTable.withColumnRenamed(header.column(expr), newColumn)
    }
    from(updatedHeader, updatedTable)
  }

  def withColumnRenamed(oldColumn: Expr, newColumn: String): R = {
    val updatedHeader = header.withColumnRenamed(oldColumn, newColumn)
    val updatedTable = table.withColumnRenamed(header.column(oldColumn), newColumn)
    from(updatedHeader, updatedTable)
  }

  def orderBy(sortItems: SortItem[Expr]*): R = {
    val tableSortItems: Seq[(String, Order)] = sortItems.map {
      case Asc(expr) => header.column(expr) -> Ascending
      case Desc(expr) => header.column(expr) -> Descending
    }
    from(header, table.orderBy(tableSortItems: _*))
  }

  def withAliases(originalToAlias: (Expr, Var)*): R = {
    val headerWithAliases = header.withAlias(originalToAlias: _*)
    from(headerWithAliases, table)
  }

  def removeVars(vars: Set[Var]): R = {
    val updatedHeader = header -- vars
    val keepColumns = updatedHeader.columns.toSeq.sorted
    val updatedData = table.select(keepColumns: _*)
    from(updatedHeader, updatedData)
  }

  def unionAll(other: R): R = {
    val unionData = table.unionAll(other.table)
    from(header, unionData)
  }

  def distinct: R = {
    from(header, table.distinct)
  }

  def distinct(fields: Var*): R = {
    from(header, table.distinct(fields.flatMap(header.expressionsFor).map(header.column).sorted: _*))
  }

  def join(other: R, joinType: JoinType, joinExprs: (Expr, Expr)*): R = {
    val joinCols = joinExprs.map { case (l, r) => header.column(l) -> other.header.column(r) }
    val joinHeader = other.header ++ header
    val joinData = table.join(other.table, joinType, joinCols: _*)
    from(joinHeader, joinData)
  }
}

/**
  * An entity table describes how to map an input data frame to a Cypher entity (i.e. nodes or relationships).
  */
trait EntityTable[T <: FlatRelationalTable[T]] extends RelationalCypherRecords[T] {

  verify()

  def schema: Schema

  def mapping: EntityMapping

  def header: RecordHeader = mapping match {
    case n: NodeMapping => headerFrom(n)
    case r: RelationshipMapping => headerFrom(r)
  }

  protected def verify(): Unit = {
    mapping.idKeys.foreach(key => table.verifyColumnType(key, CTInteger, "id key"))
    if (table.columns != mapping.allSourceKeys) throw IllegalArgumentException(
      s"Columns: ${mapping.allSourceKeys.mkString(", ")}",
      s"Columns: ${table.columns.mkString(", ")}",
      s"Use CAPS[Node|Relationship]Table#fromMapping to create a valid EntityTable")
  }

  protected def headerFrom(nodeMapping: NodeMapping): RecordHeader = {
    val nodeVar = Var("")(nodeMapping.cypherType)

    val exprToColumn = Map[Expr, String](nodeMapping.id(nodeVar)) ++
      nodeMapping.optionalLabels(nodeVar) ++
      nodeMapping.properties(nodeVar, table.columnType)

    RecordHeader(exprToColumn)
  }

  protected def headerFrom(relationshipMapping: RelationshipMapping): RecordHeader = {
    val relVar = Var("")(relationshipMapping.cypherType)

    val exprToColumn = Map[Expr, String](
      relationshipMapping.id(relVar),
      relationshipMapping.startNode(relVar),
      relationshipMapping.endNode(relVar)) ++ relationshipMapping.properties(relVar, table.columnType)

    RecordHeader(exprToColumn)
  }
}

object RelationalEntityMapping {

  implicit class EntityMappingOps(val mapping: EntityMapping) {

    def id(v: Var): (Var, String) = v -> mapping.sourceIdKey

    def properties(
      v: Var,
      columnToCypherType: Map[String, CypherType]
    ): Map[Property, String] = mapping.propertyMapping.map {
      case (key, sourceColumn) => Property(v, PropertyKey(key))(columnToCypherType(sourceColumn)) -> sourceColumn
    }
  }

  implicit class NodeMappingOps(val mapping: NodeMapping) {

    def optionalLabels(node: Var): Map[HasLabel, String] = mapping.optionalLabelMapping.map {
      case (label, sourceColumn) => HasLabel(node, Label(label))(CTBoolean) -> sourceColumn
    }
  }

  implicit class RelationshipMappingOps(val mapping: RelationshipMapping) {

    def startNode(rel: Var): (StartNode, String) = StartNode(rel)(CTNode) -> mapping.sourceStartNodeKey

    def endNode(rel: Var): (EndNode, String) = EndNode(rel)(CTNode) -> mapping.sourceEndNodeKey
  }

}
