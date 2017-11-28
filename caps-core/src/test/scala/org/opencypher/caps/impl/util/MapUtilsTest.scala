/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.util

import org.opencypher.caps.test.BaseTestSuite

class MapUtilsTest extends BaseTestSuite {

  test("merge") {
    val map1 = Map(1 -> 1, 2 -> 2)
    val map2 = Map(1 -> 2, 3 -> 3)

    MapUtils.merge(map1, map2)(_ + _) should equal(Map(1 -> 3, 2 -> 2, 3 -> 3))
  }
}
