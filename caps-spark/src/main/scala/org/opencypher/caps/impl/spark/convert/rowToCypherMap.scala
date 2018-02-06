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
package org.opencypher.caps.impl.spark.convert

import org.apache.spark.sql.Row
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.api.value._
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.impl.spark.SparkColumnName
import org.opencypher.caps.ir.api.expr.Var

final case class rowToCypherMap(header: RecordHeader) extends (Row => CypherMap) {
  override def apply(row: Row): CypherMap = {
    val values = header.internalHeader.fields.map { field =>
      field.name -> constructValue(row, field)
    }.toSeq

    CypherMap(values: _*)
  }

  private def constructValue(row: Row, field: Var): CypherValue = {
    field.cypherType match {
      case _: CTNode =>
        val (id, labels, properties) = collectNode(row, field)
        CAPSNode(id, labels.toSet, properties)

      case _: CTRelationship =>
        val (id, source, target, typ, properties) = collectRel(row, field)
        CAPSRelationship(id, source, target, typ, properties)

      case _ =>
        val raw = row.getAs[Any](SparkColumnName.of(header.slotFor(field)))
        CypherValue(raw)
    }
  }

  private def collectNode(row: Row, field: Var): (Long, Seq[String], Map[String, CypherValue]) = {
    val id = row.getAs[Long](SparkColumnName.of(header.slotFor(field)))
    val labels = header
      .labelSlots(field)
      .mapValues { s =>
        row.getAs[Boolean](SparkColumnName.of(s))
      }
      .collect {
        case (h, b) if b =>
          h.label.name
      }
      .toSeq
    val properties = header
      .propertySlots(field)
      .mapValues { s =>
        CypherValue(row.getAs[Any](SparkColumnName.of(s)))
      }
      .collect {
        case (p, v) if !v.isNull =>
          p.key.name -> v
      }

    (id, labels, properties)
  }

  private def collectRel(row: Row, field: Var): (Long, Long, Long, String, CypherMap) = {
    val id = row.getAs[Long](SparkColumnName.of(header.slotFor(field)))
    val source = row.getAs[Long](SparkColumnName.of(header.sourceNodeSlot(field)))
    val target = row.getAs[Long](SparkColumnName.of(header.targetNodeSlot(field)))
    val typ = row.getAs[String](SparkColumnName.of(header.typeSlot(field)))
    val properties = header
      .propertySlots(field)
      .mapValues { s =>
        CypherValue.apply(row.getAs[Any](SparkColumnName.of(s)))
      }
      .collect {
        case (p, v) if !v.isNull =>
          p.key.name -> v
      }

    (id, source, target, typ, properties)
  }
}
