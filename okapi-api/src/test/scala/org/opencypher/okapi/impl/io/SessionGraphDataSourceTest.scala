/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.impl.io

import org.mockito.Mockito._
import org.opencypher.okapi.ApiBaseTest
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.impl.exception.GraphNotFoundException

class SessionGraphDataSourceTest extends ApiBaseTest {

  it("hasGraph should return true for existing graph") {
    val source = new SessionGraphDataSource
    val testGraphName = GraphName("test")
    source.store(testGraphName, null)
    source.hasGraph(testGraphName) shouldBe true
  }

  it("hasGraph should return false for non-existing graph") {
    val source = new SessionGraphDataSource
    val testGraphName = GraphName("test")
    source.hasGraph(testGraphName) shouldBe false
  }

  it("graph should return graph for existing graph") {
    val source = new SessionGraphDataSource
    val testGraphName = GraphName("test")
    val testGraph = mock[PropertyGraph]
    source.store(testGraphName, testGraph)
    source.graph(testGraphName) should be(testGraph)
  }

  it("graph should throw exception for non-existing graph") {
    val source = new SessionGraphDataSource
    val testGraphName = GraphName("test")
    an[GraphNotFoundException] should be thrownBy source.graph(testGraphName)
  }

  it("schema should throw for non-existing graph") {
    val source = new SessionGraphDataSource
    val testGraphName = GraphName("test")
    a [GraphNotFoundException] shouldBe thrownBy(source.schema(testGraphName))
  }

  it("schema should return schema for existing graph") {
    val source = new SessionGraphDataSource
    val testGraphName = GraphName("test")
    val propertyGraph = mock[PropertyGraph]
    when(propertyGraph.schema).thenReturn(Schema.empty.withRelationshipType("foo"))
    source.store(testGraphName, propertyGraph)
    source.schema(testGraphName).get should be(Schema.empty.withRelationshipType("foo"))
  }

  it("graphNames should return all names of stored graphs") {
    val source = new SessionGraphDataSource
    val testGraphName1 = GraphName("test1")
    val testGraphName2 = GraphName("test2")

    source.graphNames should equal(Set.empty)

    source.store(testGraphName1, null)
    source.store(testGraphName2, null)
    source.graphNames should equal(Set(testGraphName1, testGraphName2))
  }

}
