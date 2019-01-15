package org.opencypher.spark

import java.util.{Calendar, Date}

import org.opencypher.spark.testing.CAPSTestSuite

class TimeZoneTest extends CAPSTestSuite {
  it("timezone") {
    println("foo2")
    println("Date = " + new Date())
    println("Calendar = " + Calendar.getInstance())
  }
}
