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
package org.opencypher.okapi.relational.api.table

import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader

trait RelationalCypherRecords[T <: FlatRelationalTable[T]] extends CypherRecords {

  type Records <: RelationalCypherRecords[T]

  def from(header: RecordHeader, table: T, displayNames: Option[Seq[String]] = None): Records

  def table: T

  override def physicalColumns: Seq[String] = table.physicalColumns

  def header: RecordHeader

  def cache(): Records = from(header, table.cache)

  /**
    * Align the record header to the target header and rename the stored entity to `v`.
    *
    * It is required that the `CAPSRecords` instance is a scan, meaning that it must contain exactly a single entity
    * (node or relationship) and its parts (flattened). The stored entity is renamed by this function to the argument
    * variable `v`.
    *
    * @param v            the variable that the aligned scan should contain
    * @param targetHeader the header to align with
    * @return a new instance of `CAPSRecords` aligned with the argument header
    */
  def alignWith(v: Var, targetHeader: RecordHeader): Records = {

    val entityVars = header.entityVars

    val oldEntity = entityVars.toSeq match {
      case Seq(one) => one
      case Nil => throw IllegalArgumentException("one entity in the record header", s"no entity in $header")
      case _ => throw IllegalArgumentException("one entity in the record header", s"multiple entities in $header")
    }

    assert(header.ownedBy(oldEntity) == header.expressions, s"header describes more than one entity")

    val entityLabels: Set[String] = oldEntity.cypherType match {
      case CTNode(labels, _) => labels
      case CTRelationship(types, _) => types
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", oldEntity.cypherType)
    }

    val updatedHeader = header
      .withAlias(oldEntity as v)
      .select(v)

    val missingExpressions = targetHeader.expressions -- updatedHeader.expressions
    val overlapExpressions = targetHeader.expressions -- missingExpressions

    // rename existing columns according to target header
    val dataWithColumnsRenamed = overlapExpressions.foldLeft(table) {
      case (currentTable, expr) =>
        val oldColumn = updatedHeader.column(expr)
        val newColumn = targetHeader.column(expr)
        if (oldColumn != newColumn) {
          currentTable.withColumnRenamed(oldColumn, newColumn)
        } else {
          currentTable
        }
    }

    // add missing columns
    val dataWithMissingColumns = missingExpressions.foldLeft(dataWithColumnsRenamed) {
      case (currentTable, expr) =>
        val columnName = targetHeader.column(expr)
        expr match {
          case HasLabel(_, label) =>
            if (entityLabels.contains(label.name)) {
              currentTable.withTrueColumn(columnName)
            } else {
              currentTable.withFalseColumn(columnName)
            }
          case HasType(_, relType) =>
            if (entityLabels.contains(relType.name)) {
              currentTable.withTrueColumn(columnName)
            } else {
              currentTable.withFalseColumn(columnName)
            }
          case _ =>
            if (!expr.cypherType.isNullable) {
              throw UnsupportedOperationException(
                s"Cannot align scan on $v by adding a NULL column, because the type for '$expr' is non-nullable"
              )
            }
            currentTable.withNullColumn(columnName, expr.cypherType)
        }
    }
    from(targetHeader, dataWithMissingColumns)
  }
}
