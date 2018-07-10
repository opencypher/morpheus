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
package org.opencypher.okapi.relational.api.physical

import org.opencypher.okapi.api.graph.{PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.types.CypherType._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.table.RecordHeader

/**
  * Represents a backend-specific implementation of a physical query operation on the underlying data.
  *
  * @tparam R backend-specific cypher records
  * @tparam G backend-specific property graph
  * @tparam C backend-specific runtime context
  */
trait PhysicalOperator[T <: FlatRelationalTable[T], R <: RelationalCypherRecords[T], G <: PropertyGraph, C <: RuntimeContext[T, R, G]] {

  type TagStrategy = Map[QualifiedGraphName, Map[Int, Int]]

  def context: C

  /**
    * The record header constructed by that operator.
    *
    * @return record header describing the output data
    */
  def header: RecordHeader

  def returnItems: Option[Seq[Var]] = None

  protected def _table: T

  def table: T = {
    val t = _table

    if (t.physicalColumns.toSet != header.columns) {
      // Ensure no duplicate columns in initialData
      val initialDataColumns = t.physicalColumns

      val duplicateColumns = initialDataColumns.groupBy(identity).collect {
        case (key, values) if values.size > 1 => key
      }

      if (duplicateColumns.nonEmpty)
        throw IllegalArgumentException(
          s"${getClass.getSimpleName}: a table with distinct columns",
          s"a table with duplicate columns: ${initialDataColumns.sorted.mkString("[", ", ", "]")}")

      // Verify that all header column names exist in the data
      val headerColumnNames = header.columns
      val dataColumnNames = t.physicalColumns.toSet
      val missingTableColumns = headerColumnNames -- dataColumnNames
      if (missingTableColumns.nonEmpty) {
        throw IllegalArgumentException(
          s"${getClass.getSimpleName}: data with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
          s"data with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}"
        )
      }
      // TODO: uncomment and fix expectations
//      val missingHeaderColumns = dataColumnNames -- headerColumnNames
//      if (missingHeaderColumns.nonEmpty) {
//        throw IllegalArgumentException(
//          s"data with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
//          s"data with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}"
//        )
//      }

      // Verify column types
      header.expressions.foreach { expr =>
        val tableType = t.columnType(header.column(expr))
        val headerType = expr.cypherType
        // if the type in the data doesn't correspond to the type in the header we fail
        // except: we encode nodes, rels and integers with the same data type, so we can't fail
        // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)

        headerType match {
          case _ if tableType == headerType =>
          case n: CTNode if n.subTypeOf(CTAnyNode.nullable) && tableType == CTInteger =>
          case r: CTRelationship  if r.subTypeOf(CTAnyRelationship.nullable) && tableType == CTInteger =>
          case _ => throw IllegalArgumentException(
            s"${getClass.getSimpleName}: data matching header type $headerType for expression $expr", tableType)
        }
      }
    }
    t
  }

  def graph: G

  def graphName: QualifiedGraphName

  def tagStrategy: TagStrategy = Map.empty

}
