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
package org.opencypher.caps.test.fixture

import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.support.testgraph.{GDLTestGraph, Neo4jTestGraph}
import org.scalactic.source.Position
import org.scalatest.Tag

trait TestGraphFixture extends BaseTestFixture {
  self: CAPSSessionFixture with BaseTestSuite =>

  def testWithGDL(testName: String, testTags: Tag*)
      (gdl: String)
      (testFun: GDLTestGraph => Any)
      (implicit pos: Position): Unit = test(testName, testTags: _*)(testFun(new GDLTestGraph(gdl)))


  def testWithCypher(testName: String, testTags: Tag*)
      (cypher: String)
      (testFun: Neo4jTestGraph => Any)
      (implicit pos: Position): Unit = {

    val graph = new Neo4jTestGraph(cypher)

    test(testName, testTags: _*)({
      try {
        testFun(graph)
      }
      finally {
        graph.close
      }
    })
  }
}
