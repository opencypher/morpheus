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
package org.opencypher.okapi.impl.temporal

import java.time.temporal.ChronoUnit
import java.util.Comparator
import java.util.function.ToLongFunction

import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.temporal.TemporalConstants._

/**
  * Okapi representation of a duration.
  *
  * @param months  number of months
  * @param days    number of days
  * @param seconds number of seconds
  * @param nanos   normalized number of nanoseconds spanning fractions of a second
  */
class Duration protected(val months: Long = 0, val days: Long = 0, val seconds: Long = 0, val nanos: Long = 0)
  extends Ordered[Duration] {
  def toJava: (java.time.Period, java.time.Duration) = {
    val period = java.time.Period.of((months / 12).toInt, (months % 12).toInt, days.toInt)
    val duration = java.time.Duration.ofSeconds(seconds).plus(nanos, ChronoUnit.NANOS)

    (period, duration)
  }

  override def compare(that: Duration): Int = COMPARATOR.compare(this, that)

  override def equals(o: Any): Boolean = o match {
    case d: Duration => compare(d) == 0
    case _ => false
  }
  /*
   * Since not every month has the same amount of seconds, we use the average to sum this duration in seconds.
   * Not every day has the same amount of seconds either, but since there is one day with +1 hour and one with -1 hour the average is still 24*3600 per day.
   */

  def averageLengthInSeconds: Long = {
    val daysInSeconds = Math.multiplyExact(days, SECONDS_PER_DAY)
    val monthsInSeconds = Math.multiplyExact(months, AVG_SECONDS_PER_MONTH)
    Math.addExact(seconds, Math.addExact(daysInSeconds, monthsInSeconds))
  }

  override def toString: String = s"Duration(" +
    s"months = $months, " +
    s"days = $days, " +
    s"seconds = $seconds, " +
    s"nanos = $nanos)"

  private lazy val COMPARATOR: Comparator[Duration] = {
    import scala.language.implicitConversions

    implicit def toLongFunction[T](f: T => Long): ToLongFunction[T] = new ToLongFunction[T] {
      override def applyAsLong(t: T): Long = f(t)
    }

    Comparator
      .comparingLong[Duration]((d: Duration) => d.averageLengthInSeconds)
      .thenComparingLong((d: Duration) => d.nanos)
      .thenComparingLong((d: Duration) => d.months)
      .thenComparingLong((d: Duration) => d.days)
      .thenComparingLong((d: Duration) => d.seconds)
  }
}

object Duration {

  val SUPPORTED_KEYS = Set(
    "years",
    "months",
    "weeks",
    "days",
    "hours",
    "minutes",
    "seconds",
    "milliseconds",
    "microseconds",
    "nanoseconds"
  )

  def apply(
    years: Long = 0, months: Long = 0, weeks: Long = 0, days: Long = 0,
    hours: Long = 0, minutes: Long = 0,
    seconds: Long = 0, milliseconds: Long = 0, microseconds: Long = 0, nanoseconds: Long = 0
  ): Duration = {

    val nanoSum = milliseconds * 1000000 + microseconds * 1000 + nanoseconds
    val normalizedSeconds = hours * SECONDS_PER_HOUR + minutes * SECONDS_PER_MINUTE + seconds + nanoSum / NANOS_PER_SECOND
    val normalizedNanos = nanoSum % NANOS_PER_SECOND

    new Duration(
      months = years * MONTHS_PER_YEAR + months,
      days = weeks * DAYS_PER_WEEK + days,
      seconds = normalizedSeconds,
      nanos = normalizedNanos
    )
  }

  def apply(javaDuration: java.time.Duration): Duration = {
    Duration(seconds = javaDuration.getSeconds, nanoseconds = javaDuration.getNano)
  }

  def apply(period: java.time.Period): Duration = {
    Duration(months = period.getYears * 12 + period.getMonths, days = period.getDays)
  }

  def apply(map: Map[String, Long]): Duration = {
    val sanitizedMap = map.map { case (key, value) => key.toLowerCase -> value }

    val unsupportedKeys = sanitizedMap.keySet -- SUPPORTED_KEYS
    if (unsupportedKeys.nonEmpty) {
      throw IllegalArgumentException(
        SUPPORTED_KEYS.mkString(", "),
        sanitizedMap.keySet.mkString(", "),
        s"Unsupported duration values: ${unsupportedKeys.mkString(", ")}"
      )
    }

    Duration(
      years = sanitizedMap.getOrElse("years", 0),
      months = sanitizedMap.getOrElse("months", 0),
      weeks = sanitizedMap.getOrElse("weeks", 0),
      days = sanitizedMap.getOrElse("days", 0),
      hours = sanitizedMap.getOrElse("hours", 0),
      minutes = sanitizedMap.getOrElse("minutes", 0),
      seconds = sanitizedMap.getOrElse("seconds", 0),
      milliseconds = sanitizedMap.getOrElse("milliseconds", 0),
      microseconds = sanitizedMap.getOrElse("microseconds", 0),
      nanoseconds = sanitizedMap.getOrElse("nanoseconds", 0)
    )
  }

  def parse(durationString: String): Duration = {
    val durationRegex =
      """^P(\d+Y)?(\d+M)?(\d+W)?(\d+D)?(T(\d+H)?(\d+M)?(\d+(\.\d{1,6})?S)?)?$"""
        .r("years", "months", "weeks", "days", "_", "hours", "minutes", "seconds", "_", "_")

    durationRegex.findFirstMatchIn(durationString) match {
      case Some(m) =>
        val superSecondMap = Seq("years", "months", "weeks", "days", "hours", "minutes")
          .map(id => id -> m.group(id))
          .filterNot(_._2.isNull)
          .toMap
          .mapValues(_.dropRight(1).toLong)

        val secondsMap = m.group("seconds") match {
          case s: String =>
            val doubleValue = s.dropRight(1).toDouble
            val seconds = doubleValue.toLong
            val fraction = (doubleValue - seconds) * 1000000
            val milliseconds = (fraction / 1000).toLong
            val microseconds = (fraction % 1000).toLong

            Seq(
              "seconds" -> seconds,
              "milliseconds" -> milliseconds,
              "microseconds" -> microseconds
            ).toMap

          case null => Map.empty
        }

        Duration(superSecondMap ++ secondsMap)

      case _ => throw IllegalArgumentException("a valid duration construction string", durationString)
    }
  }
}
