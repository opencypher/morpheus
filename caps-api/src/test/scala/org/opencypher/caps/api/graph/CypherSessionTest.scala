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
package org.opencypher.caps.api.graph

import org.opencypher.caps.api.io.PropertyGraphDataSource
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.impl.io.SessionPropertyGraphDataSource
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

class CypherSessionTest extends FunSuite with MockitoSugar with Matchers {

  test("avoid de-registering the session data source") {
    an[org.opencypher.caps.impl.exception.UnsupportedOperationException] should be thrownBy
      createSession.deregisterSource(SessionPropertyGraphDataSource.Namespace)
  }

  test("avoid de-registering a non-registered data source") {
    an[IllegalArgumentException] should be thrownBy createSession.deregisterSource(Namespace.from("foo"))
  }

  test("avoid retrieving a non-registered data source") {
    an[IllegalArgumentException] should be thrownBy createSession.dataSource(Namespace.from("foo"))
  }

  test("avoid retrieving a graph not stored in the session") {
    an[NoSuchElementException] should be thrownBy createSession.graph(GraphName.from("foo"))
  }

  test("avoid retrieving a graph from a non-registered data source") {
    an[IllegalArgumentException] should be thrownBy createSession.graph(QualifiedGraphName.from("foo", "bar"))
  }

  test("avoid registering a data source with an existing namespace") {
    val session = createSession
    val namespace = Namespace.from("foo")
    session.registerSource(namespace, mock[PropertyGraphDataSource])
    an[IllegalArgumentException] should be thrownBy session.registerSource(namespace, mock[PropertyGraphDataSource])
  }

  test("register data source") {
    val session = createSession
    val namespace = Namespace.from("foo")
    val dataSource = mock[PropertyGraphDataSource]
    session.registerSource(namespace, dataSource)
    session.dataSource(namespace) should equal(dataSource)
  }

  test("de-register data source") {
    val session = createSession
    val namespace = Namespace.from("foo")
    val dataSource = mock[PropertyGraphDataSource]
    session.registerSource(namespace, dataSource)
    session.dataSource(namespace) should equal(dataSource)
    session.deregisterSource(namespace)
    an[IllegalArgumentException] should be thrownBy session.dataSource(namespace)
  }

  test("namespaces") {
    val session = createSession
    session.namespaces should equal(Set(SessionPropertyGraphDataSource.Namespace))
    val namespace = Namespace.from("foo")
    val dataSource = mock[PropertyGraphDataSource]
    session.registerSource(namespace, dataSource)
    session.namespaces should equal(Set(SessionPropertyGraphDataSource.Namespace, namespace))
  }

  private def createSession: CypherSession = new CypherSession {
    override def cypher(query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]): CypherResult = ???

    override def sessionNamespace: Namespace = SessionPropertyGraphDataSource.Namespace

    override private[graph] def cypherOnGraph(graph: PropertyGraph, query: String, parameters: CypherMap, drivingTable: Option[CypherRecords]) = ???
  }
}
