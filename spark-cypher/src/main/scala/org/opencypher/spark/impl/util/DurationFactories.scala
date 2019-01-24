package org.opencypher.spark.impl.util

import java.time.Duration
import java.time.temporal.ChronoUnit

import org.apache.spark.unsafe.types.CalendarInterval

object CalendarIntervalFactory {

  /**
    * Constructs a duration representation used in Spark.
    */
  def apply(years: Long, months: Long, weeks: Long, days: Long, hours: Long, minutes: Long, seconds: Long,
    milliseconds: Long, microseconds: Long): CalendarInterval = {

    val aggregatedMonths = years * 12 + months
    var aggregatedMicroseconds = weeks * CalendarInterval.MICROS_PER_WEEK
    aggregatedMicroseconds += days * CalendarInterval.MICROS_PER_DAY
    aggregatedMicroseconds += hours * CalendarInterval.MICROS_PER_HOUR
    aggregatedMicroseconds += minutes * CalendarInterval.MICROS_PER_MINUTE
    aggregatedMicroseconds += seconds * CalendarInterval.MICROS_PER_SECOND
    aggregatedMicroseconds += milliseconds * CalendarInterval.MICROS_PER_MILLI
    aggregatedMicroseconds += microseconds
    new CalendarInterval(aggregatedMonths.toInt, aggregatedMicroseconds)
  }
}

object DurationFactory {

  /**
    * Constructs a `java.time.Duration` and handles conversions from years an months to duration.
    */
  def apply(years: Long = 0, months: Long = 0, weeks: Long = 0, days: Long = 0, hours: Long = 0, minutes: Long = 0, seconds: Long = 0, nanos: Long = 0): Duration = {
    val yearsAndMonths = ChronoUnit.MONTHS.getDuration.multipliedBy(years * 12 + months)
    val weeksAndDays = Duration.ofDays(weeks * 7 + days)
    val hourDuration = Duration.ofHours(hours)
    val minuteDuration = Duration.ofMinutes(minutes)
    val secondDuration = Duration.ofSeconds(seconds)
    val nanoDuration = Duration.ofNanos(nanos)

    yearsAndMonths plus weeksAndDays plus hourDuration plus minuteDuration plus secondDuration plus nanoDuration
  }
}