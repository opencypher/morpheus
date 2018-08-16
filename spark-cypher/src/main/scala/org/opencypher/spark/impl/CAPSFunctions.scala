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
package org.opencypher.spark.impl

import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.ArrayContains
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{Column, functions}

object CAPSFunctions {

  implicit class RichColumn(column: Column) {

    /**
      * This is a copy of {{{org.apache.spark.sql.Column#getItem}}}. The original method only allows fixed
      * values (Int, or String) as index although the underlying implementation seem capable of processing arbitrary
      * expressions. This method exposes these features
      */
    def get(idx: Column): Column = {
      new Column(UnresolvedExtractValue(column.expr, idx.expr))
    }
  }

  val rangeUdf = udf[Array[Int], Int, Int, Int]((from: Int, to: Int, step: Int) => from.to(to, step).toArray)

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

  def get_rel_type(relTypeNames: Seq[String]): UserDefinedFunction = {
    val extractRelTypes = (booleanMask: Seq[Boolean]) => filterWithMask(relTypeNames)(booleanMask)
    functions.udf(extractRelTypes.andThen(_.headOption.orNull), StringType)
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
