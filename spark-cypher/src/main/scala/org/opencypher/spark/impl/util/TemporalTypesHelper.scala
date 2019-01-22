/**
  * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
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

import java.sql.{Date, Timestamp}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, SignStyle}
import java.time.temporal.{ChronoField, IsoFields}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, MapExpression, Param}
import org.opencypher.okapi.relational.impl.table.RecordHeader

import scala.util.{Failure, Success, Try}

object TemporalTypesHelper {

  val dateTimeFormatters = Seq(
    new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("yyyyMMdd").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("yyyy-MM").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("yyyyMM")
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .toFormatter,
    DateTimeFormatter.ISO_WEEK_DATE,
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive
      .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
      .appendLiteral("W")
      .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2)
      .appendValue(ChronoField.DAY_OF_WEEK, 1)
      .toFormatter, // TODO: more week pattern variations ('YYYY-Www', 'YYYYWww')
    new DateTimeFormatterBuilder().appendPattern("yyyy")
      .appendLiteral("-Q")
      .appendValue(IsoFields.QUARTER_OF_YEAR, 1)
      .appendLiteral("-")
      .appendValue(IsoFields.DAY_OF_QUARTER, 2)
      .toFormatter, // TODO: more quarter pattern variations ('YYYYQqDD', 'YYYY-Qq', 'YYYYQq')
    new DateTimeFormatterBuilder().appendPattern("yyyy-DDD").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("yyyyDDD").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("yyyy")
      .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .toFormatter
  )

  def toDate(expr: Expr)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Date = {
    resolveArgument(expr) match {
      case Left(map) =>
        // TODO: check significance order
        val localDate = LocalDate.of(
          map.getOrElse("year", 1),
          map.getOrElse("month", 1),
          map.getOrElse("day", 1)
        )

        Date.valueOf(localDate)

      case Right(str) =>
        val matchingFormats = dateTimeFormatters.map { formatter =>
          Try {
            LocalDate.parse(str, formatter)
          } match {
            case Success(date) => Some(date)
            case Failure(err) =>
              println(err)
              None
          }
        }

        matchingFormats.collectFirst {
          case Some(date) => Date.valueOf(date)
        }.getOrElse(throw IllegalArgumentException("a date construction string that matches a valid pattern", str))

    }
  }

  def toTimestamp(expr: Expr)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Timestamp = {

    resolveArgument(expr) match {
      case Left(map) =>
        // TODO: check significance order
        val preciseTime =
          map.getOrElse("millisecond", 0) * 1000000 +
            map.getOrElse("microsecond", 0) * 1000 +
            map.getOrElse("nanosecond", 0)

        val localDateTime = LocalDateTime.of(
          map.getOrElse("year", 1),
          map.getOrElse("month", 1),
          map.getOrElse("day", 1),
          map.getOrElse("hour", 0),
          map.getOrElse("minute", 0),
          map.getOrElse("second", 0),
          preciseTime
        )

        new Timestamp(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli)

      case Right(str) => Timestamp.valueOf(str)
    }
  }

  private def resolveArgument(expr: Expr)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Either[Map[String, Int], String] = {
    expr match {
      case MapExpression(inner) =>
        val map = inner.map {
          case (key, Param(name)) => key -> (parameters(name) match {
            case CypherString(s) => s.toInt
            case CypherInteger(i) => i.toInt
            case other => throw IllegalArgumentException("A map value of type CypherString or CypherInteger", other)
          })
          case (key, expr) =>
            throw IllegalArgumentException("A valid key/value pair to construct temporal types", s"$key -> $expr")
        }

        Left(map)


      case Param(name) =>
        val s = parameters(name) match {
          case CypherString(str) => str
          case other => throw IllegalArgumentException("a CypherString", other)
        }

        Right(s)

      case other => ???
    }
  }
}
