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

import org.apache.spark.sql.catalyst.expressions.ArrayContains
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{Column, functions}

object CAPSFunctions {

  /**
    * Alternative version of `array_contains` that takes a column as the value.
    */
  def array_contains(column: Column, value: Column): Column =
    new Column(ArrayContains(column.expr, value.expr))

  def array_append_long(array: Column, value: Column): Column = {
    appendLongUDF(array, value)
  }

  private val appendLongUDF = functions.udf(appendLong _)

  private def appendLong(array: Seq[Long], element: Long): Seq[Long] = {
    array :+ element
  }

  def get_node_labels(labelNames: Seq[String]): UserDefinedFunction = {
    functions.udf(filterWithMask(labelNames) _, ArrayType(StringType, containsNull = false))
  }

  private def filterWithMask(dataToFilter: Seq[String])(mask: Seq[Boolean]): Seq[String] = {
    dataToFilter.zip(mask).collect {
      case (label, true) => label
    }
  }

  def get_property_keys(propertyKeys: Seq[String]): UserDefinedFunction = {
    functions.udf(filterNotNull(propertyKeys) _, ArrayType(StringType, containsNull = false))
  }

  private def filterNotNull(dataToFilter: Seq[String])(values: Seq[Any]): Seq[String] = {
    dataToFilter.zip(values).collect {
      case (key, value) if value != null => key
    }
  }

}
