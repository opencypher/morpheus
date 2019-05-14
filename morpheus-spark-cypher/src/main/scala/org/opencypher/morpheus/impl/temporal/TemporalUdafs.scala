/**
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
package org.opencypher.morpheus.impl.temporal

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{CalendarIntervalType, DataType, LongType, StructField, StructType}
import org.apache.spark.unsafe.types.CalendarInterval
import org.opencypher.okapi.impl.temporal.TemporalConstants

object TemporalUdafs extends Logging{

  def greaterThan(interval1 : CalendarInterval, interval2 : CalendarInterval): Boolean =
    if(avgSeconds(interval1) >= avgSeconds(interval2)) true else false

  //similar to comparison in neo4j (not the same as CalenderInterval only differs between seconds and months)
  def avgSeconds(interval : CalendarInterval): Double = {
    interval.months * TemporalConstants.AVG_DAYS_PER_MONTH + interval.microseconds / CalendarInterval.MICROS_PER_SECOND
  }

  abstract class SimpleDurationAggregation(aggrName : String) extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(Array(StructField("duration", CalendarIntervalType)))
    override def bufferSchema: StructType = StructType(Array(StructField(aggrName, CalendarIntervalType)))
    override def dataType: DataType = CalendarIntervalType
    override def deterministic: Boolean = true
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = new CalendarInterval(0, 0L)
    }
    override def evaluate(buffer: Row): Any = buffer.getAs[CalendarInterval](0)
  }

  class DurationSum extends SimpleDurationAggregation("sum") {
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[CalendarInterval](0).add(input.getAs[CalendarInterval](0))
    }
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer2.getAs[CalendarInterval](0).add(buffer1.getAs[CalendarInterval](0))
    }
  }

  class DurationMax extends SimpleDurationAggregation("max"){
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val currMaxInterval = buffer.getAs[CalendarInterval](0)
      val inputInterval = input.getAs[CalendarInterval](0)
      buffer(0) = if(greaterThan(currMaxInterval, inputInterval)) currMaxInterval else inputInterval
    }
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val interval1 = buffer1.getAs[CalendarInterval](0)
      val interval2 = buffer2.getAs[CalendarInterval](0)
      buffer1(0) = if(greaterThan(interval1, interval2)) interval1 else interval2
    }
  }

  class DurationMin extends SimpleDurationAggregation("min"){
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = new CalendarInterval(Integer.MAX_VALUE, Long.MaxValue)
    }
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val currMinInterval = buffer.getAs[CalendarInterval](0)
      val inputInterval = input.getAs[CalendarInterval](0)
      buffer(0) = if(greaterThan(inputInterval, currMinInterval)) currMinInterval else inputInterval
    }
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val interval1 = buffer1.getAs[CalendarInterval](0)
      val interval2 = buffer2.getAs[CalendarInterval](0)
      buffer1(0) = if(greaterThan(interval2, interval1)) interval1 else interval2
    }
  }

  class DurationAvg extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = StructType(Array(StructField("duration", CalendarIntervalType)))
    override def bufferSchema: StructType = StructType(Array(StructField("sum", CalendarIntervalType), StructField("cnt", LongType)))
    override def dataType: DataType = CalendarIntervalType
    override def deterministic: Boolean = true
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = new CalendarInterval(0, 0L)
      buffer(1) = 0L
    }
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[CalendarInterval](0).add(input.getAs[CalendarInterval](0))
      buffer(1) = buffer.getLong(1) + 1
    }
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer2.getAs[CalendarInterval](0).add(buffer1.getAs[CalendarInterval](0))
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
    override def evaluate(buffer: Row): Any = {
      val sumInterval = buffer.getAs[CalendarInterval](0)
      val cnt = buffer.getLong(1)
      new CalendarInterval((sumInterval.months / cnt).toInt, sumInterval.microseconds / cnt)
    }
  }

  val durationSum = new DurationSum()
  val durationAvg = new DurationAvg()
  val durationMin = new DurationMin()
  val durationMax = new DurationMax()
}
