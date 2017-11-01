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
package org.opencypher.caps.api.spark.graphx

import org.opencypher.caps.api.spark.graphx.Algorithms._
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSSession}
import org.opencypher.caps.demo.{Friend, Person}
import org.opencypher.caps.test.CAPSTestSuite

class AlgorithmsTest extends CAPSTestSuite {

  test("GraphX PageRank on example") {
    val persons = List(Person(0, "Alice"), Person(1, "Bob"), Person(2, "Carol"))
    val friendships = List(Friend(0, 0, 1, "23/01/1987"), Friend(1, 1, 2, "12/12/2009"))
    implicit val caps = CAPSSession.local()
    val graph = CAPSGraph.create(persons, friendships)
    val graphX = graph.toGraphX
    val ranks = graphX.pageRank(0.0001).vertices
    println(ranks.toLocalIterator.toList)
  }

}
