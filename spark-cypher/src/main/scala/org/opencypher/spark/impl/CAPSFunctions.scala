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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions.{lit, monotonically_increasing_id, struct, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, functions}
import org.opencypher.spark.impl.expressions.Serialize

import scala.reflect.runtime.universe.TypeTag

object CAPSFunctions {

  val NULL_LIT: Column = lit(null)
  val TRUE_LIT: Column = lit(true)
  val FALSE_LIT: Column = lit(false)
  val ONE_LIT: Column = lit(1)
  val E_LIT: Column = lit(Math.E)
  val PI_LIT: Column = lit(Math.PI)
  // See: https://issues.apache.org/jira/browse/SPARK-20193
  val EMPTY_STRUCT: Column = udf(() => new GenericRowWithSchema(Array(), StructType(Nil)), StructType(Nil))()

  implicit class RichColumn(column: Column) {

    /**
      * This is a copy of {{{org.apache.spark.sql.Column#getItem}}}. The original method only allows fixed
      * values (Int, or String) as index although the underlying implementation seem capable of processing arbitrary
      * expressions. This method exposes these features
      */
    def get(idx: Column): Column =
      new Column(UnresolvedExtractValue(column.expr, idx.expr))
  }

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
    new Column(Serialize(columns.map(_.expr)))
  }

  def regex_match(text: Column, pattern: Column): Column = new Column(RLike(text.expr, pattern.expr))

  def get_array_item(array: Column, index: Int): Column = {
    new Column(GetArrayItem(array.expr, functions.lit(index).expr))
  }

  private val x: NamedLambdaVariable = NamedLambdaVariable("x", StructType(Seq(StructField("item", StringType), StructField("flag", BooleanType))), nullable = false)
  private val TRUE_EXPR: Expression = functions.lit(true).expr

  def filter_true[T: TypeTag](items: Seq[T], mask: Seq[Column]): Column = {
    filter_with_mask(items, mask, LambdaFunction(EqualTo(GetStructField(x, 1), TRUE_EXPR), Seq(x), hidden = false))
  }

  def filter_not_null[T: TypeTag](items: Seq[T], mask: Seq[Column]): Column = {
    filter_with_mask(items, mask, LambdaFunction(IsNotNull(GetStructField(x, 1)), Seq(x), hidden = false))
  }

  private def filter_with_mask[T: TypeTag](items: Seq[T], mask: Seq[Column], predicate: LambdaFunction): Column = {
    require(items.size == mask.size, s"Array filtering requires for the items and the mask to have the same length.")
    if (items.isEmpty) {
      functions.array()
    } else {
      val itemLiterals = functions.array(items.map(functions.typedLit): _*)
      val zippedArray = functions.arrays_zip(itemLiterals, functions.array(mask: _*))
      val filtered = ArrayFilter(zippedArray.expr, predicate)
      val transform = ArrayTransform(filtered, LambdaFunction(GetStructField(x, 0), Seq(x), hidden = false))
      new Column(transform)
    }
  }

  // See: https://issues.apache.org/jira/browse/SPARK-20193
  def create_struct(structColumns: Seq[Column]): Column = {
    if (structColumns.isEmpty) EMPTY_STRUCT
    else struct(structColumns: _*)
  }

  /**
    * Alternative version of {{{org.apache.spark.sql.functions.translate}}} that takes {{{org.apache.spark.sql.Column}}}s for search and replace strings.
    */
  def translate(src: Column, matchingString: Column, replaceString: Column): Column = {
    new Column(StringTranslate(src.expr, matchingString.expr, replaceString.expr))
  }

}
