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
package org.opencypher.spark.impl.util

import org.apache.spark.sql.{Column, DataFrame, functions}
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, MapExpression, Param}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object TemporalTypesHelper {

  def sanitize(arg: Expr)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Column = {
    val dateIdentifiers = Seq("year", "month", "day")
    val timeIdentifiers = Seq("hour", "minute", "second")
    val preciseTimeIdentifiers = Seq("millisecond", "microsecond", "nanosecond")
    arg match {
      case MapExpression(inner) => {
        val innerAsString = inner.map {
          case (key, Param(name)) => key -> (parameters(name) match {
            case CypherString(s) => s
            case CypherInteger(i) => i.toString
            case other => throw IllegalArgumentException("A map value of type CypherString or CypherInteger", other)
          })
          case (key, expr) =>
            throw IllegalArgumentException("A valid key/value pair to construct temporal types", s"$key -> $expr")
        }
        val dates = dateIdentifiers.map(id => innerAsString.getOrElse(id, "01"))
        val times = timeIdentifiers.map(id => innerAsString.getOrElse(id, "00"))
        val preciseTime = preciseTimeIdentifiers.map(id => innerAsString.getOrElse(id, "000"))

        val formattedDate = dates.reduce(_ + "-" + _)
        val formattedTime = times.reduce(_ + ":" + _)
        val formattedPreciseTime = preciseTime.reduce(_ + "" + _)

        functions.lit(sanitizeTemporalString(s"${formattedDate}T${formattedTime}.${formattedPreciseTime}"))
      }

      case Param(name) => {
        val s = parameters(name) match {
          case CypherString(s) => s
          case other => throw IllegalArgumentException("a CypherString", other)
        }
        functions.lit(sanitizeTemporalString(s))
      }

      case other => ???
    }
  }

  private def sanitizeTemporalString(temporal: String): String = {
    temporal.split("T").toList match {
      case head :: Nil => sanitizeDate(head)
      case head :: tail => {
        val date = sanitizeDate(head)
        assert(tail.size == 1, "The character `T` should only appear once in a temporal type string.")
        val timeAndTimezone = tail.head.split("[Z+-]").toList match {
          case head :: Nil => sanitizeTime(head)
          case head :: tail => {
            val time = sanitizeTime(head)
            assert(tail.size == 1, "The characters `Z`, `+`, `-`, should only appear once in a temporal type string.")
            time + sanitizeTimezone(tail.head)
          }
          case Nil => ""
        }
        date + timeAndTimezone
      }
      case Nil => ""
    }
  }

  private def sanitizeDate(date: String): String = {
    assert(!date.contains('Q'), "Quarter representation in temporal types is not supported")
    assert(!date.contains('W'), "Week representation in temporal types is not supported")

    date.split('-').toList match {
      case year :: month :: day :: Nil =>
        s"$year-$month-$day"

      case year :: month :: Nil => month.length match {
        case 2 => s"$year-$month-01"
        case 3 => ??? // construct month from days: 202 -> 07-21
        case other => ???
      }

      case date :: Nil => date.length match {
        case 4 => s"$date-01-01"
        case 6 => {
          val year = date.substring(0, 4)
          val month = date.substring(4)
          s"$year-$month-01"
        }
        case 7 => ??? // construct month from days: 202 -> 07-21
        case 8 => {
          val year = date.substring(0, 4)
          val month = date.substring(4, 6)
          val day = date.substring(6)
          s"$year-$month-$day"
        }
      }

      case Nil => "0001-01-01"
      case head :: tail => ???
    }
  }

  private def sanitizeTime(time: String): String = s" $time"

  private def sanitizeTimezone(timezone: String): String = "Ztimezone"

}
