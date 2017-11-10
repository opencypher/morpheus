/*
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
package org.opencypher.caps.impl

import org.apache.spark.sql.DataFrame
import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.spark.CAPSSession
import org.opencypher.caps.impl.record.{AdditiveUpdateResult, SuccessfulUpdateResult}
import org.opencypher.caps.ir.api.IRField
import org.apache.spark.sql.Column

import scala.language.implicitConversions

package object util {
  type SuccessfulAdditiveUpdateResult[T] = SuccessfulUpdateResult[T] with AdditiveUpdateResult[T]

  implicit def toVar(f: IRField): Var = Var(f.name)(f.cypherType)

  implicit def toVars(fields: Set[IRField]): Set[Var] = fields.map(toVar)

  implicit class ColumnMappableDf(df: DataFrame) {
    def mapColumn(columnName: String)(f: Column => Column)(implicit caps: CAPSSession): DataFrame = {
      val tmpColName = caps.temporaryColumnName()
      df.withColumn(tmpColName, f(df(columnName))).drop(columnName).withColumnRenamed(tmpColName, columnName)
    }
  }

}
