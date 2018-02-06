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
package org.opencypher.caps.impl.spark

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.opencypher.caps.api.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.api.value._
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.impl.spark.physical.RuntimeContext
import org.opencypher.caps.ir.api.expr.{Expr, Param}

object DfUtils {

  implicit class CypherRow(r: Row) {
    def getCypherValue(expr: Expr, header: RecordHeader)(implicit context: RuntimeContext): CypherValue = {
      expr match {
        case Param(name) => context.parameters(name)
        case _ =>
          header.slotsFor(expr).headOption match {
            case None => throw IllegalArgumentException(s"slot for $expr")
            case Some(slot) =>
              val index = slot.index
              CypherValue(r.get(index))
          }
      }
    }
  }

  implicit class ColumnMappableDf(df: DataFrame) {
    def mapColumn(columnName: String)(f: Column => Column): DataFrame = {
      val tmpColName = SparkColumnName.tempColName
      df.withColumn(tmpColName, f(df(columnName))).drop(columnName).withColumnRenamed(tmpColName, columnName)
    }
  }

}
