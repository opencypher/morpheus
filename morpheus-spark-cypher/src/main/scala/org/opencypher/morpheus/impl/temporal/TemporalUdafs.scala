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
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.CalendarIntervalEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.unsafe.types.CalendarInterval
import org.opencypher.morpheus.impl.temporal.TemporalConversions._

object TemporalUdafs extends Logging {

  private val intervalEncoder = ExpressionEncoder(CalendarIntervalEncoder)

  trait SimpleDurationAggregation extends Aggregator[CalendarInterval, CalendarInterval, CalendarInterval] {
    final override def finish(reduction: CalendarInterval): CalendarInterval = reduction
    final override def bufferEncoder: Encoder[CalendarInterval] = intervalEncoder
    final override def outputEncoder: Encoder[CalendarInterval] = intervalEncoder
  }

  object DurationSum extends SimpleDurationAggregation {
    override def zero: CalendarInterval = new CalendarInterval(0, 0, 0L)
    override def reduce(b: CalendarInterval, a: CalendarInterval): CalendarInterval = IntervalUtils.add(b, a)
    override def merge(b1: CalendarInterval, b2: CalendarInterval): CalendarInterval = IntervalUtils.add(b1, b2)
  }

  object DurationMax extends SimpleDurationAggregation {
    override def zero: CalendarInterval = new CalendarInterval(0, 0, 0L)

    override def reduce(b: CalendarInterval, a: CalendarInterval): CalendarInterval = {
      if (b.toDuration.compare(a.toDuration) >= 0) b else a
    }

    override def merge(b1: CalendarInterval, b2: CalendarInterval): CalendarInterval = reduce(b1, b2)
  }

  object DurationMin extends SimpleDurationAggregation {
    final override def zero: CalendarInterval = new CalendarInterval(Int.MaxValue, Int.MaxValue, Long.MaxValue)

    override def reduce(b: CalendarInterval, a: CalendarInterval): CalendarInterval = {
      if (b.toDuration.compare(a.toDuration) >= 0) a else b
    }

    override def merge(b1: CalendarInterval, b2: CalendarInterval): CalendarInterval = reduce(b1, b2)
  }

  case class DurationAvgRunningSum(months: Int, days: Int, micros: Long, count: Long)

  object DurationAvg extends Aggregator[CalendarInterval, DurationAvgRunningSum, CalendarInterval] {
    override def zero: DurationAvgRunningSum = DurationAvgRunningSum(0, 0, 0, 0)

    override def reduce(b: DurationAvgRunningSum, a: CalendarInterval): DurationAvgRunningSum = DurationAvgRunningSum(
      months = b.months + a.months,
      days = b.days + a.days,
      micros = b.micros + a.microseconds,
      count = b.count + 1
    )

    override def merge(b1: DurationAvgRunningSum, b2: DurationAvgRunningSum): DurationAvgRunningSum = {
      DurationAvgRunningSum(
        months = b1.months + b2.months,
        days = b1.days + b2.days,
        micros = b1.micros + b2.micros,
        count = b1.count + b2.count
      )
    }

    override def finish(reduction: DurationAvgRunningSum): CalendarInterval =
      IntervalUtils.divideExact(new CalendarInterval(reduction.months, reduction.days, reduction.micros), reduction.count)

    override def bufferEncoder: Encoder[DurationAvgRunningSum] = Encoders.product[DurationAvgRunningSum]
    override def outputEncoder: Encoder[CalendarInterval] = intervalEncoder
  }
}
