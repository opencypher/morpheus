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
import org.apache.spark.unsafe.types.CalendarInterval
import org.opencypher.okapi.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.IllegalArgumentException

import scala.util.{Failure, Success, Try}

/**
  * Converts expressions into Temporal values as described in
  * [[https://github.com/opencypher/openCypher/blob/master/cip/1.accepted/CIP2015-08-06-date-time.adoc the corresponding CIP]]
  */
object TemporalTypesHelper {

  type MapOrString = Either[Map[String, Int], String]

  val dateByMonthIdentifiers: Seq[String] = Seq("year", "month", "day")
  val dateByWeekIdentifiers: Seq[String] = Seq("year", "week", "dayofweek")
  val dateByOrdinalDayIdentifiers: Seq[String] = Seq("year", "ordinalday")
  val dateByQuarterIdentifiers: Seq[String] = Seq("year", "quarter", "dayofquarter")
  val timeIdentifiers: Seq[String] = Seq("hour", "minute", "second")

  val dateFormatters: Seq[DateTimeFormatter] = Seq(
    // 2010-01-01
    new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter,

    // 20100101
    new DateTimeFormatterBuilder().appendPattern("yyyyMMdd").toFormatter,

    // 2010-10
    new DateTimeFormatterBuilder().appendPattern("yyyy-MM")
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .toFormatter,

    // 201010
    new DateTimeFormatterBuilder().appendPattern("yyyyMM")
      .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
      .toFormatter,

    // 2015-W30-2
    DateTimeFormatter.ISO_WEEK_DATE,

    // 2015W302
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive
      .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
      .appendLiteral("W")
      .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2)
      .appendValue(ChronoField.DAY_OF_WEEK, 1)
      .toFormatter,

    // 2015-W30
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive
      .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
      .appendLiteral("-")
      .appendLiteral("W")
      .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2)
      .parseDefaulting(ChronoField.DAY_OF_WEEK, 1)
      .toFormatter,

    // 2015W30
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive
      .appendValue(IsoFields.WEEK_BASED_YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
      .appendLiteral("W")
      .appendValue(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 2)
      .parseDefaulting(ChronoField.DAY_OF_WEEK, 1)
      .toFormatter,

    // 2015-Q2-60
    new DateTimeFormatterBuilder().appendPattern("yyyy")
      .appendLiteral("-Q")
      .appendValue(IsoFields.QUARTER_OF_YEAR, 1)
      .appendLiteral("-")
      .appendValue(IsoFields.DAY_OF_QUARTER, 2)
      .toFormatter,

    // 2015Q260
    new DateTimeFormatterBuilder().appendPattern("yyyy")
      .appendLiteral("Q")
      .appendValue(IsoFields.QUARTER_OF_YEAR, 1)
      .appendValue(IsoFields.DAY_OF_QUARTER, 2)
      .toFormatter,

    // 2015-Q2
    new DateTimeFormatterBuilder().appendPattern("yyyy")
      .appendLiteral("-Q")
      .appendValue(IsoFields.QUARTER_OF_YEAR, 1)
      .parseDefaulting(IsoFields.DAY_OF_QUARTER, 1)
      .toFormatter,

    // 2015Q2
    new DateTimeFormatterBuilder().appendPattern("yyyy")
      .appendLiteral("Q")
      .appendValue(IsoFields.QUARTER_OF_YEAR, 1)
      .parseDefaulting(IsoFields.DAY_OF_QUARTER, 1)
      .toFormatter,

    // 2015-202
    new DateTimeFormatterBuilder().appendPattern("yyyy-DDD").toFormatter,

    // 2015202
    new DateTimeFormatterBuilder().appendPattern("yyyyDDD").toFormatter,

    // 2015
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

  def parseDate(mapOrString: MapOrString): Date = {
    val localDate = mapOrString match {
      case Left(map) => parseDateMap(map)
      case Right(str)=> parseDateString(str)
    }

    Date.valueOf(localDate)
  }

  def parseTimestamp(mapOrString: MapOrString): Timestamp = {

    mapOrString match {
      case Left(map) =>

        val date = parseDateMap(map)
        val time = parseTimeMap(map)
        val dateTime = LocalDateTime.of(date, time)

        Timestamp.valueOf(dateTime)

      case Right(str) =>
        val dateString :: timeString = str.split("T", 2).toList

        val date = parseDateString(dateString)
        val maybeTime = timeString match {
          case t :: Nil => Some(parseTimeString(t))
          case _ => None
        }

        val dateTime = maybeTime match {
          case Some(time) => LocalDateTime.of(date, time)
          case None => LocalDateTime.of(date, LocalTime.MIN)
        }

        Timestamp.valueOf(dateTime)
    }
  }

  def toDuration(expr: Expr)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): Option[CalendarInterval] = {
    resolveArgument(expr).map { value =>
      val durationMap = value match {
        case Left(map) => map.mapValues(_.toLong)

        case Right(str) =>
          val durationRegex =
            """^P(\d+Y)?(\d+M)?(\d+W)?(\d+D)?(T(\d+H)?(\d+M)?(\d+(\.\d{1,6})?S)?)?$"""
              .r("years", "months", "weeks", "days", "_", "hours", "minutes", "seconds", "_", "_")

          durationRegex.findFirstMatchIn(str) match {
            case Some(m) =>
              val withoutSeconds = Seq("years", "months", "weeks", "days", "hours", "minutes")
                .map(id => id -> m.group(id))
                .filterNot(_._2.isNull)
                .toMap
                .mapValues(_.dropRight(1).toLong)

              m.group("seconds") match {
                case s: String =>
                  val doubleValue = s.dropRight(1).toDouble
                  val seconds = doubleValue.toLong
                  val fraction = (doubleValue - seconds) * 1000000
                  val milliseconds = (fraction / 1000).toLong
                  val microseconds = (fraction % 1000).toLong

                  withoutSeconds ++ Seq(
                    "seconds" -> seconds,
                    "milliseconds" -> milliseconds,
                    "microseconds" -> microseconds
                  ).toMap

                case null => withoutSeconds
              }

            case _ => throw IllegalArgumentException("a valid duration construction string", str)
          }
      }

      CalendarIntervalFactory(
        durationMap.getOrElse("years", 0),
        durationMap.getOrElse("months", 0),
        durationMap.getOrElse("weeks", 0),
        durationMap.getOrElse("days", 0),
        durationMap.getOrElse("hours", 0),
        durationMap.getOrElse("minutes", 0),
        durationMap.getOrElse("seconds", 0),
        durationMap.getOrElse("milliseconds", 0),
        durationMap.getOrElse("microseconds", 0)
      )
    }
  }

  private def parseDateMap(map: Map[String, Int]): LocalDate = {
    val sanitizedMap = sanitizeMap(map)

    if(!sanitizedMap.contains("year")) throw IllegalArgumentException("the key `year` needs to be set", map.keys.mkString(", "))

    if (sanitizedMap.keySet.contains("week")) {
      checkSignificanceOrder(sanitizedMap, dateByWeekIdentifiers)

      LocalDate.MIN
        .`with`(IsoFields.WEEK_BASED_YEAR, sanitizedMap("year"))
        .`with`(IsoFields.WEEK_OF_WEEK_BASED_YEAR, sanitizedMap("week"))
        .`with`(ChronoField.DAY_OF_WEEK, sanitizedMap.getOrElse("dayofweek", 1).toLong)

    } else if (sanitizedMap.keySet.contains("ordinalday")) {
      checkSignificanceOrder(sanitizedMap, dateByOrdinalDayIdentifiers)

      LocalDate.ofYearDay(sanitizedMap("year"), sanitizedMap("ordinalday"))

    } else if (sanitizedMap.keySet.contains("quarter")) {
      checkSignificanceOrder(sanitizedMap, dateByQuarterIdentifiers)

      LocalDate.MIN
        .withYear(sanitizedMap("year"))
        .`with`(IsoFields.QUARTER_OF_YEAR, sanitizedMap("quarter"))
        .`with`(IsoFields.DAY_OF_QUARTER, sanitizedMap.getOrElse("dayofquarter", 1).toLong)

    }
    else {
      checkSignificanceOrder(sanitizedMap, dateByMonthIdentifiers)

      LocalDate.of(sanitizedMap("year"), sanitizedMap.getOrElse("month", 1), sanitizedMap.getOrElse("day", 1))
    }
  }

  private def parseDateString(str: String): LocalDate = {
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
      case None => throw IllegalArgumentException("a valid date construction string", str)
    }
  }

  private def parseTimeMap(map: Map[String, Int]): LocalTime = {
    val sanitizedMap = sanitizeMap(map)

    checkSignificanceOrder(sanitizedMap, timeIdentifiers)

    val preciseTime =
      sanitizedMap.getOrElse("millisecond", 0) * 1000000 +
        sanitizedMap.getOrElse("microsecond", 0) * 1000 +
        sanitizedMap.getOrElse("nanosecond", 0)

    LocalTime.of(
      sanitizedMap.getOrElse("hour", 0),
      sanitizedMap.getOrElse("minute", 0),
      sanitizedMap.getOrElse("second", 0),
      preciseTime
    )
  }

  private def parseTimeString(str: String): LocalTime = {
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
      case None => throw IllegalArgumentException("a valid time construction string", str)
    }
  }

  private def checkSignificanceOrder(inputMap: Map[String, _], keys: Seq[String]): Unit = {
    val validOrder = keys
      .map(inputMap.isDefinedAt)
      .sliding(2)
      .forall {
        case false :: true :: Nil => false
        case _ => true
      }

    if(!validOrder) throw IllegalArgumentException(
      "a valid significance order",
      inputMap.keys.mkString(", "),
      "When constructing dates from a map it is forbidden to omit values of higher significance"
    )
  }

  def sanitizeMap(map: Map[String, Int]): Map[String, Int] = map.map {
    case (key, value) => key.toLowerCase -> value
  }
}
