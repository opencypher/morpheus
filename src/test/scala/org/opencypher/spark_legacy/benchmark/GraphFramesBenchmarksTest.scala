/**
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
package org.opencypher.spark_legacy.benchmark

import org.opencypher.spark_legacy.impl.{In, Out}
import org.opencypher.spark.BaseTestSuite

class GraphFramesBenchmarksTest extends BaseTestSuite {

  test("motif construction") {
    GraphFramesBenchmarks.buildMotif(Seq(Out("") -> "")) should equal("(n0)-[r0]->(n1)")
    GraphFramesBenchmarks.buildMotif(Seq(Out("") -> "", Out("") -> "")) should equal("(n0)-[r0]->(n1); (n1)-[r1]->(n2)")
    GraphFramesBenchmarks.buildMotif(Seq(Out("") -> "", In("") -> "")) should equal("(n0)-[r0]->(n1); (n2)-[r1]->(n1)")
    GraphFramesBenchmarks.buildMotif(Seq(Out("") -> "", In("") -> "", In("") -> "")) should equal("(n0)-[r0]->(n1); (n2)-[r1]->(n1); (n3)-[r2]->(n2)")

    an [IllegalArgumentException] should be thrownBy {
      GraphFramesBenchmarks.buildMotif(Seq(In("") -> ""))
    }
  }

}
