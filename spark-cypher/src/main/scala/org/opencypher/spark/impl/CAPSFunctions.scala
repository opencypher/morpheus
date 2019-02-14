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
package org.opencypher.spark.impl

import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.{ArrayContains, StringTranslate, XxHash64}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{monotonically_increasing_id, udf}
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{Column, functions}
import org.opencypher.spark.impl.expressions.Serialize

object CAPSFunctions {

  implicit class RichColumn(column: Column) {

    /**
      * This is a copy of {{{org.apache.spark.sql.Column#getItem}}}. The original method only allows fixed
      * values (Int, or String) as index although the underlying implementation seem capable of processing arbitrary
      * expressions. This method exposes these features
      */
    def get(idx: Column): Column =
      new Column(UnresolvedExtractValue(column.expr, idx.expr))
  }

  val rangeUdf: UserDefinedFunction =
    udf[Array[Int], Int, Int, Int]((from: Int, to: Int, step: Int) => from.to(to, step).toArray)

  private[spark] val rowIdSpaceBitsUsedByMonotonicallyIncreasingId = 33

  /**
    * Configurable wrapper around `monotonically_increasing_id`
    *
    * @param partitionStartDelta Conceptually this number is added to the `partitionIndex` from which the Spark function
    *                            `monotonically_increasing_id` starts assigning IDs.
    */
  // TODO: Document inherited limitations with regard to the maximum number of rows per data frame
  // TODO: Document the maximum number of partitions (before entering tag space)
  def partitioned_id_assignment(partitionStartDelta: Int): Column =
    monotonically_increasing_id() + (partitionStartDelta.toLong << rowIdSpaceBitsUsedByMonotonicallyIncreasingId)

  /**
    * Alternative version of `array_contains` that takes a column as the value.
    */
  def array_contains(column: Column, value: Column): Column =
    new Column(ArrayContains(column.expr, value.expr))

  def hash64(columns: Column*): Column =
    new Column(new XxHash64(columns.map(_.expr)))

  def serialize(columns: Column*): Column = {
    //TODO: Enable check once bug in SQLPGDS is fixed
    //    columns.foreach(c => assert(!c.expr.nullable, s"Nullable column $c cannot be serialized"))
    new Column(Serialize(columns.map(_.expr)))
  }

  def array_append_long(array: Column, value: Column): Column =
    appendLongUDF(array, value)

  private val appendLongUDF =
    functions.udf(appendLong _)

  private def appendLong(array: Seq[Long], element: Long): Seq[Long] =
    array :+ element

  def get_rel_type(relTypeNames: Seq[String]): UserDefinedFunction = {
    val extractRelTypes = (booleanMask: Seq[Boolean]) => filterWithMask(relTypeNames)(booleanMask)
    functions.udf(extractRelTypes.andThen(_.headOption.orNull), StringType)
  }

  def get_node_labels(labelNames: Seq[String]): UserDefinedFunction =
    functions.udf(filterWithMask(labelNames) _, ArrayType(StringType, containsNull = false))

  private def filterWithMask(dataToFilter: Seq[String])(mask: Seq[Boolean]): Seq[String] =
    dataToFilter.zip(mask).collect {
      case (label, true) => label
    }

  def get_property_keys(propertyKeys: Seq[String]): UserDefinedFunction =
    functions.udf(filterNotNull(propertyKeys) _, ArrayType(StringType, containsNull = false))

  private def filterNotNull(dataToFilter: Seq[String])(values: Seq[Any]): Seq[String] =
    dataToFilter.zip(values).collect {
      case (key, value) if value != null => key
    }

  /**
    * Alternative version of {{{org.apache.spark.sql.functions.translate}}} that takes {{{org.apache.spark.sql.Column}}}s for search and replace strings.
    */
  def translateColumn(src: Column, matchingString: Column, replaceString: Column): Column = {
    new Column(StringTranslate(src.expr, matchingString.expr, replaceString.expr))
  }

}
