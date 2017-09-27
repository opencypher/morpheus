/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark

import org.apache.spark.sql.Row
import org.opencypher.caps.api.expr.{Const, Expr}
import org.opencypher.caps.api.record.RecordHeader
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value._
import org.opencypher.caps.api.value.instances._
import org.opencypher.caps.impl.spark.exception.Raise
import org.opencypher.caps.impl.spark.physical.RuntimeContext

object RowUtils {

  implicit class CypherRow(r: Row) {
    def getCypherValue(expr: Expr, header: RecordHeader)(implicit context: RuntimeContext): CypherValue = {
      expr match {
        case c: Const => context.parameters(context.constants.constantRef(c.constant))
        case _ =>
          header.slotsFor(expr).headOption match {
            case None => Raise.slotNotFound(expr.toString)
            case Some(slot) =>
              val index = slot.index

              if (r.isNullAt(index))
                null
              else typeToValue(slot.content.cypherType.material)(r.get(index))
          }
      }
    }

    def typeToValue(t: CypherType): Any => CypherValue = t match {
      case CTBoolean => (in) => cypherBoolean(in.asInstanceOf[Boolean])
      case CTInteger => (in) => cypherInteger(in.asInstanceOf[Long])
      case CTString => (in) => cypherString(in.asInstanceOf[String])
      case CTFloat => (in) => cypherFloat(in.asInstanceOf[Double])
      case CTNode => (in) => cypherInteger(in.asInstanceOf[Long])
      // TODO: This supports var-expand where we only track rel ids, but it's not right
      case CTRelationship => (in) => cypherInteger(in.asInstanceOf[Long])
      case l: CTList => (in) => {
        val converted = in.asInstanceOf[Seq[_]].map(typeToValue(l.elementType))

        cypherList(converted.toIndexedSeq)
      }
      case _ => Raise.notYetImplemented(s"converting value of type $t")
    }
  }
}
