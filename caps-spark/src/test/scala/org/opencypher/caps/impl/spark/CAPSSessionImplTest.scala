/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.impl.spark

import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.TeamDataFixture

import scala.collection.Bag

class CAPSSessionImplTest extends CAPSTestSuite with TeamDataFixture {

  it("can execute sql on registered tables") {
    CAPSRecords.wrap(personDF).register("people")
    CAPSRecords.wrap(knowsDF).register("knows")

    val sqlResult = caps.sql(
      """
        |SELECT people.name AS me, knows.since AS since, p2.name AS you
        |FROM people
        |INNER JOIN knows ON knows.src = people.id
        |INNER JOIN people p2 ON knows.dst = p2.id
      """.stripMargin)

    sqlResult.iterator.toBag should equal(Bag(
      CypherMap("me" -> "Mats", "since" -> 2017, "you" -> "Martin"),
      CypherMap("me" -> "Mats", "since" -> 2016, "you" -> "Max"),
      CypherMap("me" -> "Mats", "since" -> 2015, "you" -> "Stefan"),
      CypherMap("me" -> "Martin", "since" -> 2016, "you" -> "Max"),
      CypherMap("me" -> "Martin", "since" -> 2013, "you" -> "Stefan"),
      CypherMap("me" -> "Max", "since" -> 2016, "you" -> "Stefan")
    ))
  }

}
