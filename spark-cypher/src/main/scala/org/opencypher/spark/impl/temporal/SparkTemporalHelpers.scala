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
package org.opencypher.spark.impl.temporal

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.unsafe.types.CalendarInterval
import org.opencypher.okapi.impl.temporal.Duration

object SparkTemporalHelpers extends Logging{
  implicit class RichDuration(duration: Duration) {

    /**
      * Converts the Okapi representation of a duration into the spark representation.
      * @note This conversion is lossy, as the Sparks [[CalendarInterval]] only has a resolution down to microseconds.
      *       Additionally it uses an approximate representation of days.
      */
    def toCalendarInterval: CalendarInterval = {
      if (duration.nanos % 1000 != 0) {
        logger.warn("Spark does not support durations with nanosecond resolution, truncating!")
      }

      val microseconds = duration.nanos / 1000 +
                         duration.seconds * CalendarInterval.MICROS_PER_SECOND +
                         duration.days * CalendarInterval.MICROS_PER_DAY

      new CalendarInterval(
        duration.months.toInt,
        microseconds
      )
    }
  }

  /**
    * Converts the Spark representation of a duration into the Okapi representation.
    * @note To ensure compatibility with the reverse operation we estimate the number of days from the given seconds.
    */
  implicit class RichCalendarInterval(calendarInterval: CalendarInterval) {
    def toDuration: Duration = {
      val seconds = calendarInterval.microseconds / CalendarInterval.MICROS_PER_SECOND
      val normalizedDays = seconds / ( CalendarInterval.MICROS_PER_DAY / CalendarInterval.MICROS_PER_SECOND )
      val normalizedSeconds = seconds % ( CalendarInterval.MICROS_PER_DAY / CalendarInterval.MICROS_PER_SECOND )
      val normalizedNanos = calendarInterval.microseconds % CalendarInterval.MICROS_PER_SECOND * 1000

      Duration(months = calendarInterval.months,
        days = normalizedDays,
        seconds = normalizedSeconds,
        nanoseconds = normalizedNanos
      )
    }
  }
}
