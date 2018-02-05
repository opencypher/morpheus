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
package org.opencypher.caps.api.value

import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.api.value.CypherValue._

class CypherValueTest extends BaseTestSuite {

  val aString = "a"
  val cypherString = CypherValue(aString)
  val aLong = 10L
  val cypherInteger = CypherValue(aLong)
  val aMap = Map("s" -> cypherString, "i" -> cypherInteger)
  val cypherMap = CypherValue(aMap)

  val cypherValues = List(cypherString, cypherInteger, cypherMap)

  test("cast to Cypher value") {
    // Safe
    cypherString.cast[String] should equal(aString)
    cypherInteger.cast[Long] should equal(aLong)
    cypherMap.cast[Map[String, CypherValue]] should equal(aMap)
  }

  test("cast to value") {
    cypherString.cast[CypherString].value should equal(aString)
  }

  test("unwrap") {
    cypherString.unwrap should equal(aString)
  }

}
