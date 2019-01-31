/*
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
package org.opencypher.spark.impl.acceptance

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.junit.runner.RunWith
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, UnsupportedOperationException}
import org.opencypher.okapi.impl.temporal.Duration
import org.opencypher.okapi.testing.Bag
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TemporalTests extends CAPSTestSuite with ScanGraphInit {
  private def shouldParseDate(given: String, expected: String): Unit = {
    caps.cypher(s"RETURN date('$given') AS time").records.toMapsWithCollectedEntities should equal(
      Bag(CypherMap("time" -> java.time.LocalDate.parse(expected)))
    )
  }

  private def shouldParseDateTime(given: String, expected: String): Unit = {
    caps.cypher(s"RETURN localdatetime('$given') AS time").records.toMapsWithCollectedEntities should equal(
      Bag(CypherMap("time" -> java.time.LocalDateTime.parse(expected)))
    )
  }

  describe("duration") {
    it("parses cypher compatible duration strings") {
      caps.cypher("RETURN duration('P1Y2M20D') AS duration").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(years = 1, months = 2, days = 20)))
      )

      caps.cypher("RETURN duration('PT1S') AS duration").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(seconds = 1)))
      )

      caps.cypher("RETURN duration('PT111.123456S') AS duration").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(seconds = 111, nanoseconds = 123456000)))
      )

      caps.cypher("RETURN duration('PT1M10S') AS duration").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(minutes = 1, seconds = 10)))
      )

      caps.cypher("RETURN duration('PT3H1M10S') AS duration").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(hours = 3, minutes = 1, seconds = 10)))
      )

      caps.cypher("RETURN duration('P5DT3H1M10S') AS duration").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(days = 5, hours = 3, minutes = 1, seconds = 10)))
      )

      caps.cypher("RETURN duration('P1W5DT3H1M10S') AS duration")
        .records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(weeks = 1, days = 5, hours = 3, minutes = 1, seconds = 10)))
      )

      caps.cypher("RETURN duration('P12M1W5DT3H1M10S') AS duration")
        .records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(months = 12, weeks = 1, days = 5, hours = 3, minutes = 1,
          seconds = 10)))
      )

      caps.cypher("RETURN duration('P3Y12M1W5DT3H1M10S') AS duration")
        .records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(years = 3, months = 12, weeks = 1, days = 5, hours = 3,
          minutes = 1, seconds = 10)))
      )
    }

    it("constructs duration from a map") {
      caps.cypher("RETURN duration({ seconds: 1 }) AS duration").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("duration" -> Duration(seconds = 1)))
      )

      caps.cypher(
        """RETURN duration({
          | years: 3,
          | months: 12,
          | weeks: 1,
          | days: 5,
          | hours: 3,
          | minutes: 1,
          | seconds: 10,
          | milliseconds: 10,
          | microseconds: 10 }) AS duration""".stripMargin).records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("duration" -> Duration(
            years = 3, months = 12, weeks = 1, days = 5,
            hours = 3, minutes = 1, seconds = 10, milliseconds = 10, microseconds = 10)
          )
        )
      )
    }

    describe("addition") {
      it("supports addition to duration") {
        caps.cypher("RETURN duration('P1D') + duration('P1D') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> Duration(days = 2)))
        )
      }

      it("supports addition to date") {
        caps.cypher("RETURN date('2010-10-10') + duration('P1D') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> java.time.LocalDate.parse("2010-10-11")))
        )
      }

      it("supports addition to date with with time part present") {
        caps.cypher("RETURN date('2010-10-10') + duration('P1DT12H') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> java.time.LocalDate.parse("2010-10-11")))
        )
      }

      it("supports addition to localdatetime") {
        caps.cypher("RETURN localdatetime('2010-10-10T12:00') + duration('P1D') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> java.time.LocalDateTime.parse("2010-10-11T12:00:00")))
        )
      }

      it("supports addition to localdatetime with with time part present") {
        caps.cypher("RETURN localdatetime('2010-10-10T12:00') + duration('P1DT12H') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> java.time.LocalDateTime.parse("2010-10-12T00:00:00")))
        )
      }
    }

    describe("subtraction") {
      it("supports subtraction to duration") {
        caps.cypher("RETURN duration('P1D') - duration('P1D') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> Duration()))
        )

        caps.cypher("RETURN duration('P1D') - duration('PT12H') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> Duration(hours = 12)))
        )
      }

      it("supports subtraction to date") {
        caps.cypher("RETURN date('2010-10-10') - duration('P1D') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> java.time.LocalDate.parse("2010-10-09")))
        )

        caps.cypher(
          """
            |WITH
            | date({year: 1984, month: 10, day: 11}) AS date,
            | duration({months: 1, days: -14}) as duration
            |RETURN date - duration AS diff
          """.stripMargin).records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("diff" -> java.time.LocalDate.parse("1984-09-25")))
        )
      }

      it("supports subtraction to date with with time part present") {
        caps.cypher("RETURN date('2010-10-10') - duration('P1DT12H') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> java.time.LocalDate.parse("2010-10-09")))
        )
      }

      it("supports subtraction to localdatetime") {
        caps.cypher("RETURN localdatetime('2010-10-10T12:00') - duration('P1D') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> java.time.LocalDateTime.parse("2010-10-09T12:00:00")))
        )
      }

      it("supports subtraction to localdatetime with with time part present") {
        caps.cypher("RETURN localdatetime('2010-10-10T12:00') - duration('P1DT12H') AS time").records.toMapsWithCollectedEntities should equal(
          Bag(CypherMap("time" -> java.time.LocalDateTime.parse("2010-10-09T00:00:00")))
        )
      }
    }
  }

  describe("date") {

    it("returns a valid date") {
      caps.cypher("RETURN date('2010-10-10') AS time").records.toMaps should equal(
        Bag(CypherMap("time" -> java.time.LocalDate.parse("2010-10-10")))
      )
    }

    it("parses cypher compatible date strings") {
      Seq(
        "2010-10-10" -> "2010-10-10",
        "20101010" -> "2010-10-10",
        "2010-12" -> "2010-12-01",
        "201012" -> "2010-12-01",
        "2015-W30-2" -> "2015-07-21",
        "2015W302" -> "2015-07-21",
        "2015-W30" -> "2015-07-20",
        "2015W30" -> "2015-07-20",
        "2015-Q2-60" -> "2015-05-30",
        "2015Q260" -> "2015-05-30",
        "2015-Q2" -> "2015-04-01",
        "2015Q2" -> "2015-04-01",
        "2015-202" -> "2015-07-21",
        "2015202" -> "2015-07-21",
        "2010" -> "2010-01-01").foreach {
        case (given, expected) => shouldParseDate(given, expected)
      }
    }

    it("returns a valid date when constructed from a map") {
      caps.cypher("RETURN date({ year: 2010, month: 10, day: 10 }) AS time").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("time" -> java.time.LocalDate.parse("2010-10-10")))
      )

      caps.cypher("RETURN date({ year: 2010, month: 10 }) AS time").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("time" -> java.time.LocalDate.parse("2010-10-01")))
      )

      caps.cypher("RETURN date({ year: '2010' }) AS time").records.toMapsWithCollectedEntities should equal(
        Bag(CypherMap("time" -> java.time.LocalDate.parse("2010-01-01")))
      )
    }

    it("throws an error if values of higher significance are omitted") {
      val e1 = the[IllegalArgumentException] thrownBy
        caps.cypher("RETURN date({ year: 2018, day: 356 })").records.toMapsWithCollectedEntities
      e1.getMessage should (include("valid significance order") and include("year, day"))

      val e2 = the[IllegalArgumentException] thrownBy
        caps.cypher("RETURN date({ month: 11, day: 2 })").records.toMapsWithCollectedEntities
      e2.getMessage should (include("`year` needs to be set") and include("month, day"))

      val e3 = the[IllegalArgumentException] thrownBy
        caps.cypher("RETURN date({ day: 2 })").records.toMapsWithCollectedEntities
      e3.getMessage should (include("`year` needs to be set") and include("day"))
    }

    it("throws an error if the date argument is wrong") {
      val e1 = the[IllegalArgumentException] thrownBy
        caps.cypher("RETURN date('2018-10-10-10')").records.toMapsWithCollectedEntities
      e1.getMessage should (include("valid date construction string") and include("2018-10-10-10"))

      val e2 = the[IllegalArgumentException] thrownBy
        caps.cypher("RETURN date('201810101')").records.toMapsWithCollectedEntities
      e2.getMessage should (include("valid date construction string") and include("201810101"))
    }

    it("compares two dates") {
      caps.cypher("RETURN date('2015-10-10') < date('2015-10-12') AS time").records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("time" -> true)
        )
      )

      caps.cypher("RETURN date('2015-10-10') > date('2015-10-12') AS time").records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("time" -> false)
        )
      )
    }

    it("returns current date if no parameters are given") {
      val currentDate = new java.sql.Date(System.currentTimeMillis()).toString
      caps.cypher(s"RETURN date('$currentDate') <= date() AS time").records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("time" -> true)
        )
      )
    }

    it("should propagate null") {
      caps.cypher("RETURN date(null) as time").records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("time" -> null)
        )
      )
    }
  }

  describe("localdatetime") {

    it("parses cypher compatible localdatetime strings") {
      Seq(
        "2010-10-10" -> "2010-10-10T00:00:00",
        "20101010" -> "2010-10-10T00:00:00",
        "2010-12" -> "2010-12-01T00:00:00",
        "201012" -> "2010-12-01T00:00:00",
        "2015-W30-2" -> "2015-07-21T00:00:00",
        "2015W302" -> "2015-07-21T00:00:00",
        "2015-W30" -> "2015-07-20T00:00:00",
        "2015W30" -> "2015-07-20T00:00:00",
        "2015-Q2-60" -> "2015-05-30T00:00:00",
        "2015Q260" -> "2015-05-30T00:00:00",
        "2015-Q2" -> "2015-04-01T00:00:00",
        "2015Q2" -> "2015-04-01T00:00:00",
        "2015-202" -> "2015-07-21T00:00:00",
        "2015202" -> "2015-07-21T00:00:00",
        "2010" -> "2010-01-01T00:00:00",
        "2010-10-10T21:40:32.142" -> "2010-10-10T21:40:32.142",
        "2010-10-10T214032.142" -> "2010-10-10T21:40:32.142",
        "2010-10-10T21:40:32" -> "2010-10-10T21:40:32",
        "2010-10-10T214032" -> "2010-10-10T21:40:32",
        "2010-10-10T21:40" -> "2010-10-10T21:40:00",
        "2010-10-10T2140" -> "2010-10-10T21:40:00",
        "2010-10-10T21" -> "2010-10-10T21:00:00"
      ).foreach {
        case (given, expected) => shouldParseDateTime(given, expected)
      }
    }

    it("returns a valid localdatetime when constructed from a map") {
      caps.cypher(
        """RETURN localdatetime({
          |year: 2015,
          |month: 10,
          |day: 12,
          |hour: 12,
          |minute: 50,
          |second: 35,
          |millisecond: 556}) AS time""".stripMargin).records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("time" -> java.time.LocalDateTime.parse("2015-10-12T12:50:35.556"))
        )
      )

      caps.cypher(
        """RETURN localdatetime({
          |year: 2015,
          |month: 10,
          |day: 1,
          |hour: 12,
          |minute: 50}) AS time""".stripMargin).records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("time" -> java.time.LocalDateTime.parse("2015-10-01T12:50:00"))
        )
      )
    }

    it("throws an error if nanoseconds are specified") {
      val e = the[IllegalStateException] thrownBy caps.cypher(
        """RETURN localdatetime({
          |year: 2015,
          |month: 10,
          |day: 1,
          |hour: 12,
          |minute: 50,
          |second: 1,
          |nanosecond: 42}) AS time""".stripMargin).records.toMapsWithCollectedEntities

      e.getMessage should include("nanosecond resolution")
    }

    it("throws an error if values of higher significance are omitted") {
      val e1 = the[IllegalArgumentException] thrownBy caps.cypher("RETURN localdatetime({year: 2011, minute: 50 })").records.toMapsWithCollectedEntities
      e1.getMessage should (include("valid significance order") and include("minute"))

      val e2 = the[IllegalArgumentException] thrownBy caps.cypher("RETURN localdatetime({ year: 2018, hour: 12, second: 14 })").records.toMapsWithCollectedEntities
      e2.getMessage should (include("valid significance order") and include("year, hour, second"))
    }

    it("throws an error if the localdatetime string is malformed") {
      val e1 = the[IllegalArgumentException] thrownBy
        caps.cypher("RETURN localdatetime('2018-10-10T12:10:30:15')").records.toMapsWithCollectedEntities
      e1.getMessage.should(include("valid time construction string") and include("12:10:30:15"))

      val e2 = the[IllegalArgumentException] thrownBy
        caps.cypher("RETURN localdatetime('20181010T1210301')").records.toMapsWithCollectedEntities
      e2.getMessage should (include("valid time construction string") and include("1210301"))

      val e3 = the[IllegalArgumentException] thrownBy
        caps.cypher("RETURN localdatetime('20181010123123T12:00')").records.toMapsWithCollectedEntities
      e3.getMessage should (include("valid date construction string") and include("20181010123123"))
    }

    it("compares two datetimes") {
      caps.cypher("RETURN localdatetime('2015-10-10T00:00:00') < localdatetime('2015-10-12T00:00:00') AS time").records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("time" -> true)
        )
      )

      caps.cypher("RETURN localdatetime('2015-10-10T00:00:00') > localdatetime('2015-10-12T00:00:00') AS time").records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("time" -> false)
        )
      )
    }

    it("uses the current date and time if no parameters are given") {
      val currentDateTime = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
      caps.cypher(s"RETURN localdatetime('$currentDateTime') <= localdatetime() AS time").records.toMapsWithCollectedEntities equals equal(
        Bag(
          CypherMap("time" -> true)
        )
      )
    }

    it("should propagate null") {
      caps.cypher("RETURN localdatetime(null) as time").records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("time" -> null)
        )
      )
    }
  }

  describe("temporal accessors") {
    it("propagates null values") {
      caps.cypher("""RETURN date(null).year AS year""").records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("year" -> null)
        )
      )

      caps.cypher("""RETURN localdatetime(null).year AS year""").records.toMapsWithCollectedEntities should equal(
        Bag(
          CypherMap("year" -> null)
        )
      )
    }

    it("throws an error for unknown accessors") {
      val e = the[UnsupportedOperationException] thrownBy caps.cypher("""RETURN date().foo AS foo""").records.toMapsWithCollectedEntities

      e.getMessage should include("foo")
    }

    describe("year") {
      it("works on date") {
        caps.cypher("""RETURN date('2015-10-10').year AS year""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("year" -> 2015)
          )
        )
      }

      it("works on localdatetime") {
        caps.cypher("""RETURN localdatetime('2015-10-10T10:10').year AS year""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("year" -> 2015)
          )
        )
      }
    }

    describe("quarter") {
      it("works on date") {
        caps.cypher("""RETURN date('2015-10-10').quarter AS quarter""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("quarter" -> 4)
          )
        )
      }

      it("works on localdatetime") {
        caps.cypher("""RETURN localdatetime('2015-10-10T10:10').quarter AS quarter""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("quarter" -> 4)
          )
        )
      }
    }

    describe("month") {
      it("works on date") {
        caps.cypher("""RETURN date('2015-10-10').month AS month""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("month" -> 10)
          )
        )
      }

      it("works on localdatetime") {
        caps.cypher("""RETURN localdatetime('2015-10-10T10:10').month AS month""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("month" -> 10)
          )
        )
      }
    }

    describe("week") {
      it("works on date") {
        caps.cypher("""RETURN date('2019-01-01').week AS week""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("week" -> 1)
          )
        )
      }

      it("works on localdatetime") {
        caps.cypher("""RETURN localdatetime('2019-01-01T10:10').week AS week""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("week" -> 1)
          )
        )
      }
    }

    describe("weekYear") {
      it("works on date") {
        caps.cypher("""RETURN date('1813-01-01').weekYear AS weekYear""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("weekYear" -> 1812)
          )
        )
      }

      it("works on localdatetime") {
        caps.cypher("""RETURN localdatetime('1813-01-01T10:10').weekYear AS weekYear""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("weekYear" -> 1812)
          )
        )
      }
    }

    describe("dayOfQuarter") {
      it("works on date") {
        caps.cypher("""RETURN date('2019-01-01').dayOfQuarter AS dayOfQuarter""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("dayOfQuarter" -> 1)
          )
        )
      }

      it("works on localdatetime") {
        caps.cypher("""RETURN localdatetime('2019-01-01T10:10').dayOfQuarter AS dayOfQuarter""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("dayOfQuarter" -> 1)
          )
        )
      }
    }

    describe("day") {
      it("works on date") {
        caps.cypher("""RETURN date('2019-05-10').day AS day""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("day" -> 10)
          )
        )
      }

      it("works on localdatetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:10').day AS day""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("day" -> 10)
          )
        )
      }
    }

    describe("ordinalDay") {
      it("works on date") {
        caps.cypher("""RETURN date('2019-05-10').ordinalDay AS ordinalDay""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("ordinalDay" -> 130)
          )
        )
      }

      it("works on localdatetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:10').ordinalDay AS ordinalDay""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("ordinalDay" -> 130)
          )
        )
      }
    }

    describe("dayOfWeek") {
      it("works on date") {
        caps.cypher("""RETURN date('2019-05-10').dayOfWeek AS dayOfWeek""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("dayOfWeek" -> 5)
          )
        )
      }

      it("works on localdatetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:10').dayOfWeek AS dayOfWeek""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("dayOfWeek" -> 5)
          )
        )
      }
    }

    describe("hour") {
      it("works on datetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:10').hour AS hour""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("hour" -> 10)
          )
        )
      }
    }

    describe("minute") {
      it("works on datetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:11').minute AS minute""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("minute" -> 11)
          )
        )
      }
    }

    describe("second") {
      it("works on datetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:10:12').second AS second""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("second" -> 12)
          )
        )
      }
    }

    describe("millisecond") {
      it("works on datetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:10:12.113').millisecond AS millisecond""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("millisecond" -> 113)
          )
        )
      }
    }

    describe("microsecond") {
      it("works on datetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:10:12.113114').microsecond AS microsecond""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("microsecond" -> 113114)
          )
        )
      }
    }

    describe("epochMillis") {
      it("works on datetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:10:12.113114').epochMillis AS epochMillis""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("epochMillis" -> 1.557475812113114E12)
          )
        )
      }
    }

    describe("epochSeconds") {
      it("works on datetime") {
        caps.cypher("""RETURN localdatetime('2019-05-10T10:10:12.113114').epochSeconds AS epochSeconds""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("epochSeconds" -> 1.557475812113114E9)
          )
        )
      }
    }

    describe("duration based accessors") {
      it("supports years") {
        caps.cypher("""RETURN duration({years: 2, months: 14}).years AS years""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("years" -> 3)
          )
        )
      }

      it("supports months") {
        caps.cypher("""RETURN duration({years: 2, months: 14}).months AS months""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("months" -> 38)
          )
        )
      }

      it("supports weeks") {
        caps.cypher("""RETURN duration({years: 2, days: 15}).weeks AS weeks""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("weeks" -> 2)
          )
        )
      }

      it("supports days") {
        caps.cypher("""RETURN duration({years: 2, days: 14}).days AS days""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("days" -> 14)
          )
        )
      }

      it("supports hours") {
        caps.cypher("""RETURN duration({years: 2, hours: 5, minutes: 60}).hours AS hours""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("hours" -> 6)
          )
        )
      }

      it("supports minutes") {
        caps.cypher("""RETURN duration({years: 2, hours: 2, minutes: 2}).minutes AS minutes""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("minutes" -> 122)
          )
        )
      }

      it("supports seconds") {
        caps.cypher("""RETURN duration({years: 2, minutes: 1, seconds: 2}).seconds AS seconds""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("seconds" -> 62)
          )
        )
      }

      it("supports milliseconds") {
        caps.cypher("""RETURN duration({years: 2, milliseconds: 142}).milliseconds AS millis""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("millis" -> 142)
          )
        )
      }

      it("supports microseconds") {
        caps.cypher("""RETURN duration({years: 2, microseconds: 142}).microseconds AS micros""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("micros" -> 142)
          )
        )
      }

      // not supported as Spark CalendarInterval does not support nanoseconds
      ignore("supports nanoseconds") {
        caps.cypher("""RETURN duration({years: 2, microseconds: 1}).nanoseconds AS nanos""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("nanos" -> 1000)
          )
        )
      }

      it("propagates null") {
        caps.cypher("""RETURN duration(null).years AS years""").records.toMapsWithCollectedEntities should equal(
          Bag(
            CypherMap("years" -> null)
          )
        )
      }
    }
  }
}
