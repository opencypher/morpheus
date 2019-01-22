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

import org.opencypher.okapi.api.graph.{GraphName, Namespace}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.relational.api.graph.RelationalCypherGraph
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.impl.table.SparkTable
import org.opencypher.spark.testing.CAPSTestSuite

class QualifiedGraphNameAcceptance extends CAPSTestSuite with ScanGraphInit {

  val defaultGraph: RelationalCypherGraph[SparkTable.DataFrameTable] = initGraph("CREATE (:A)-[:REL]->(:B)")

  def defaultDS: SessionGraphDataSource = {
    val ds = new SessionGraphDataSource()
    ds.store(GraphName("foo"), defaultGraph)
    ds.store(GraphName("foo.bar"), defaultGraph)
    ds.store(GraphName("my best graph"), defaultGraph)
    ds
  }

  caps.registerSource(Namespace("foo"), defaultDS)
  caps.registerSource(Namespace("foo.bar"), defaultDS)
  caps.registerSource(Namespace("my best data source"), defaultDS)

  describe("FROM GRAPH") {
    def assertFromGraph(namespace: String, graphName: String) = {
      caps.cypher(
        s"""
           |FROM GRAPH $namespace.$graphName
           |MATCH (n)
           |RETURN COUNT(n) as cnt
        """.stripMargin
      ).records.iterator.toBag should equal(Bag(
        CypherMap("cnt" -> 2)
      ))
    }

    it("can load from escaped namespaces") {
      assertFromGraph("`foo.bar`", "foo")
      assertFromGraph("`my best data source`", "foo")
    }

    it("can load from escaped graph names") {
      assertFromGraph("foo", "`foo.bar`")
      assertFromGraph("foo", "`my best graph`")
    }

    it("can load from escaped namespace and graph name") {
      assertFromGraph("`foo.bar`", "`foo.bar`")
      assertFromGraph("`my best data source`", "`my best graph`")
    }

    it("can load from escaped graph name with default namespace") {
      val sessionDS = caps.catalog.source(caps.catalog.sessionNamespace)
      sessionDS.store(GraphName("my best graph"), defaultGraph)

      caps.cypher(
        s"""
           |FROM GRAPH `my best graph`
           |MATCH (n)
           |RETURN COUNT(n) as cnt
        """.stripMargin
      ).records.iterator.toBag should equal(Bag(
        CypherMap("cnt" -> 2)
      ))
    }
  }

  describe("CONSTRUCT ON") {
    def assertConstructOn(namespace: String, graphName: String) = {
      caps.cypher(
        s"""
           |CONSTRUCT ON $namespace.$graphName
           |MATCH (n)
           |RETURN COUNT(n) as cnt
        """.stripMargin
      ).records.iterator.toBag should equal(Bag(
        CypherMap("cnt" -> 2)
      ))
    }

    it("can construct on escaped namespaces") {
      assertConstructOn("`foo.bar`", "foo")
      assertConstructOn("`my best data source`", "foo")
    }

    it("can construct on escaped graph names") {
      assertConstructOn("foo", "`foo.bar`")
      assertConstructOn("foo", "`my best graph`")
    }

    it("can construct on escaped namespace and graph name") {
      assertConstructOn("`foo.bar`", "`foo.bar`")
      assertConstructOn("`my best data source`", "`my best graph`")
    }

    it("can construct on expaced graph name and default namespace") {
      val sessionDS = caps.catalog.source(caps.catalog.sessionNamespace)
      sessionDS.store(GraphName("my best graph"), defaultGraph)

      caps.cypher(
        s"""
           |CONSTRUCT ON `my best graph`
           |MATCH (n)
           |RETURN COUNT(n) as cnt
        """.stripMargin
      ).records.iterator.toBag should equal(Bag(
        CypherMap("cnt" -> 2)
      ))
    }
  }

  describe("CATALOG CREATE GRAPH") {
    def assertCreateGraph(namespace: String, graphName: String) = {
      caps.cypher(
        s"""
           |CATALOG CREATE GRAPH $namespace.$graphName {
           | CONSTRUCT ON foo.foo
           | RETURN GRAPH
           |}
          """.stripMargin
      )

      caps.catalog
        .source(Namespace(namespace.replaceAll("`", "")))
        .hasGraph(GraphName(graphName.replaceAll("`", ""))) should be(true)
    }

    it("can create in escaped namespaces") {
      assertCreateGraph("`foo.bar`", "construct1")
      assertCreateGraph("`my best data source`", "construct1")
    }

    it("can create a graph with escaped graph names") {
      assertCreateGraph("foo", "`foo.bar.construct`")
      assertCreateGraph("foo", "`my constructed graph`")
    }

    it("can create in escaped namespace and graph name") {
      assertCreateGraph("`foo.bar`", "`foo.bar.construct`")
      assertCreateGraph("`my best data source`", "`my best constructed graph`")
    }

    it("can create a graph with escaped graph name in default source ") {
      caps.cypher(
        s"""
           |CATALOG CREATE GRAPH `my best constructed graph` {
           | CONSTRUCT ON foo.foo
           | RETURN GRAPH
           |}
          """.stripMargin
      )

      caps
        .catalog.source(caps.catalog.sessionNamespace)
        .hasGraph(GraphName("my best constructed graph")) should be(true)
    }
  }
}
