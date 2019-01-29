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

import java.sql.Date

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.unsafe.types.CalendarInterval

object TemporalUDFS extends Logging {

  /**
    * Subtracts a duration from a date.
    * Duration components on a sub-day level are ignored
    */
  val dateAdd: UserDefinedFunction =
    udf[Date, Date, CalendarInterval]((date: Date, interval: CalendarInterval) =>{
      if(date == null || interval == null) {
        null
      } else {
        val days = interval.microseconds / CalendarInterval.MICROS_PER_DAY

        if(interval.microseconds % CalendarInterval.MICROS_PER_DAY != 0) {
          logger.warn("Arithmetic with Date and Duration can lead to incorrect results when sub-day values are present.")
        }

        val reducedLocalDate = date
          .toLocalDate
          .plusMonths(interval.months)
          .plusDays(days)

        Date.valueOf(reducedLocalDate)
      }
    })

  /**
    * Subtracts a duration from a date.
    * Duration components on a sub-day level are ignored
    */
  val dateSubtract: UserDefinedFunction =
    udf[Date, Date, CalendarInterval]((date: Date, interval: CalendarInterval) =>{
      if(date == null || interval == null) {
        null
      } else {
        val days = interval.microseconds / CalendarInterval.MICROS_PER_DAY

        if(interval.microseconds % CalendarInterval.MICROS_PER_DAY != 0) {
          logger.warn("Arithmetic with Date and Duration can lead to incorrect results when sub-day values are present.")
        }

        val reducedLocalDate = date
          .toLocalDate
          .minusMonths(interval.months)
          .minusDays(days)

        Date.valueOf(reducedLocalDate)
      }
    })
}
