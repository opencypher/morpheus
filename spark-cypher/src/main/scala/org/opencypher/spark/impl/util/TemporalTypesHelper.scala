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
import java.time.{LocalDate, LocalDateTime, LocalTime}

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{Expr, MapExpression, NullLit, Param}
import org.opencypher.okapi.relational.impl.table.RecordHeader

import scala.util.{Failure, Success, Try}

object TemporalTypesHelper {

  val dateIdentifiers: Seq[String] = Seq("year", "month", "day")
  val timeIdentifiers: Seq[String] = Seq("hour", "minute", "second", "millisecond", "microsecond", "nanosecond")

  val dateFormatters: Seq[DateTimeFormatter] = Seq(
    new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("yyyyMMdd").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("yyyy-MM")
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .toFormatter,
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

  val timeFormatters: Seq[DateTimeFormatter] = Seq(
    new DateTimeFormatterBuilder().appendPattern("HH:mm:ss.SSS").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("HHmmss.SSS").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("HH:mm:ss").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("HHmmss").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("HH:mm").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("HHmm").toFormatter,
    new DateTimeFormatterBuilder().appendPattern("HH").toFormatter
  )

  def toDate(expr: Expr)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Option[Date] = {

    resolveArgument(expr) match {
      case Some(Left(map)) =>

        checkSignificanceOrder(map, dateIdentifiers)

        val localDate = LocalDate.of(
          map.getOrElse("year", 1),
          map.getOrElse("month", 1),
          map.getOrElse("day", 1)
        )

        Some(Date.valueOf(localDate))

      case Some(Right(str)) => Some(Date.valueOf(parseDate(str)))

      case None => None

    }
  }

  def toTimestamp(expr: Expr)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Option[Timestamp] = {

    resolveArgument(expr) match {
      case Some(Left(map)) =>

        checkSignificanceOrder(map, dateIdentifiers ++ timeIdentifiers)

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

        Some(Timestamp.valueOf(localDateTime))

      case Some(Right(str)) =>
        val dateString :: timeString = str.split("T", 2).toList

        val date = parseDate(dateString)
        val maybeTime = timeString match {
          case t :: Nil => Some(parseTime(t))
          case _ => None
        }

        val dateTime = maybeTime match {
          case Some(time) => LocalDateTime.of(date, time)
          case None => LocalDateTime.of(date, LocalTime.MIN)
        }

        Some(Timestamp.valueOf(dateTime))

      case None => None
    }
  }

  private def parseDate(str: String): LocalDate = {
    val matchingDateFormats = dateFormatters.map { formatter =>
      Try {
        LocalDate.parse(str, formatter)
      } match {
        case Success(date) => Some(date)
        case Failure(_) => None
      }
    }
    matchingDateFormats.find(_.isDefined).flatten match {
      case Some(matchingDate) => matchingDate
      case None => throw IllegalArgumentException("a date construction string that matches a valid pattern", str)
    }
  }

  private def parseTime(str: String): LocalTime = {
    val matchingTimeFormats = timeFormatters.map { formatter =>
      Try {
        LocalTime.parse(str, formatter)
      } match {
        case Success(date) => Some(date)
        case Failure(_) => None
      }
    }
    matchingTimeFormats.find(_.isDefined).flatten match {
      case Some(matchingTime) => matchingTime
      case None => throw IllegalArgumentException("a time construction string that matches a valid pattern", str)
    }
  }

  private def checkSignificanceOrder(inputMap: Map[String, _], keys: Seq[String]): Unit = {
    val occurences = keys.map(inputMap.isDefinedAt)
    val validOrder = occurences.tail.foldLeft(Seq((occurences.head, true))) {
      case (acc, true) if !acc.last._1 => acc :+ ((true, false))
      case (acc, current) => acc :+ ((current, true))
    }.map(_._2).reduce(_ && _)

    if(!validOrder) throw IllegalArgumentException("When constructing dates from a map it is forbidden to omit values of higher significance")
  }

  private def resolveArgument(expr: Expr)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Option[Either[Map[String, Int], String]] = {
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

        Some(Left(map))


      case Param(name) =>
        val s = parameters(name) match {
          case CypherString(str) => str
          case other => throw IllegalArgumentException("a CypherString", other)
        }

        Some(Right(s))

      case NullLit(_) => None

      case other => ???
    }
  }
}
