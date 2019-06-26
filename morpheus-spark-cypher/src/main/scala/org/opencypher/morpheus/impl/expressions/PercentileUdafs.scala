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
package org.opencypher.morpheus.impl.expressions

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.opencypher.okapi.impl.exception.IllegalArgumentException

import scala.collection.mutable


// As abs(percentile_rank() - given_percentage) inside min() is not allowed
object PercentileUdafs extends Logging {

  abstract class PercentileAggregation(percentile: Double) extends UserDefinedAggregateFunction {
    def inputSchema: StructType = StructType(Array(StructField("value", DoubleType)))
    def bufferSchema: StructType = StructType(Array(StructField("array_buffer", ArrayType(DoubleType, containsNull = false))))
    def deterministic: Boolean = true
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = Array[DoubleType]()
    }
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (input(0) != null) {
        buffer(0) = buffer(0).asInstanceOf[mutable.WrappedArray[DoubleType]] :+ input(0)
      }
    }
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1(0).asInstanceOf[mutable.WrappedArray[Double]] ++ buffer2(0).asInstanceOf[mutable.WrappedArray[Double]]
    }
  }

  class PercentileDisc(percentile: Double, numberType: DataType) extends PercentileAggregation(percentile) {
    def dataType: DataType = numberType

    def evaluate(buffer: Row): Any = {
      val sortedValues = buffer(0).asInstanceOf[mutable.WrappedArray[Double]].sortWith(_ < _)
      if (sortedValues.isEmpty) return null

      val position = (sortedValues.length * percentile).round.toInt
      val result = if (position == 0) sortedValues(0) else sortedValues(position - 1)
      dataType match {
        case LongType => result.toLong
        case DoubleType => result
        case e => throw IllegalArgumentException("a Integer or a Float", e)
      }
    }

  }

  class PercentileCont(percentile: Double) extends PercentileAggregation(percentile) {
    def dataType: DataType = DoubleType

    def evaluate(buffer: Row): Any = {
      val sortedValues = buffer(0).asInstanceOf[mutable.WrappedArray[Double]].sortWith(_ < _)
      if (sortedValues.isEmpty) return null

      val exact_position = 1 + ((sortedValues.length - 1) * percentile)
      val prec = exact_position.floor.toInt
      val succ = exact_position.ceil.toInt
      val weight = succ - exact_position
      exact_position match {
        case pos if pos < 1 => (1 - weight) * sortedValues(succ) + weight * sortedValues(prec)
        case pos if pos == succ => sortedValues(prec - 1)
        case _ => (1 - weight) * sortedValues(succ - 1) + weight * sortedValues(prec - 1)
      }
    }
  }

  def percentileDisc(percentile: Double, numberType: DataType) = new PercentileDisc(percentile, numberType: DataType)
  def percentileCont(percentile: Double) = new PercentileCont(percentile)
}
